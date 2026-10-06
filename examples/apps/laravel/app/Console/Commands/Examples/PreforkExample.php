<?php

namespace App\Console\Commands\Examples;

use App\Examples\ExampleCommand;
use App\Examples\Journal;
use App\Examples\Processes;
use App\Jobs\RecordWorker;

// docs:start(app-laravel-prefork)
final class PreforkExample extends ExampleCommand
{
    protected $signature = 'example:prefork';

    protected $description = 'What prefork changes, under php artisan queen:supervise';

    private const JOBS_PER_POOL = 4;

    private int $master;

    /** @var array<string, string> the queue of each pool of config/queen.php */
    private array $queues;

    protected function example(): void
    {
        // `default` follows the prefork switch; `isolated` has its own
        // 'prefork' => false.
        $this->queues = [
            'default' => $this->freshQueue('prefork-default'),
            'isolated' => $this->freshQueue('prefork-isolated'),
        ];
        $this->master = $this->startArtisan("{$this->queues['default']}-supervisor", ['queen:supervise'], [
            'QUEEN_SUPERVISOR_PREFORK' => 'true',
            'EXAMPLE_FORKED_QUEUE' => $this->queues['default'],
            'EXAMPLE_SPAWNED_QUEUE' => $this->queues['isolated'],
        ]);
        $this->line("\nstarted php artisan queen:supervise (pid {$this->master}) with prefork on");

        $tree = $this->waitFor('two forked and two spawned workers', 30, fn () => $this->bootedTree());
        $this->printTree($tree);
        $this->check(
            count($tree['forked']) === 2,
            'the pool `default` has 2 workers forked by the fork server',
        );
        $this->check(
            count($tree['spawned']) === 2,
            'the pool `isolated` has 2 workers spawned by the master',
        );
        $this->printMemory($tree);

        // Jobs for both pools: each records the pid of its worker and that
        // worker's parent.
        $this->line("\n" . self::JOBS_PER_POOL . ' jobs to each pool');
        foreach ($this->queues as $queue) {
            for ($n = 0; $n < self::JOBS_PER_POOL; $n++) {
                RecordWorker::dispatch()->onQueue($queue);
            }
        }
        foreach ($this->queues as $pool => $queue) {
            $ran = $this->waitForEvents($queue, 'ran', self::JOBS_PER_POOL, 30);
            $pids = array_values(array_unique(array_column($ran, 'pid')));
            sort($pids);
            $parents = array_values(array_unique(array_column($ran, 'ppid')));
            [$workers, $parent, $parentRole] = $pool === 'default'
                ? [$tree['forked'], $tree['forkServer'], 'the fork server']
                : [$tree['spawned'], $this->master, 'the master'];
            $this->line(sprintf(
                '  %-9s ran on %s, children of %s',
                $pool,
                implode(' and ', $pids),
                implode(', ', $parents) . ($parents === [$parent] ? " ({$parentRole})" : ''),
            ));
            $this->check(
                array_diff($pids, array_keys($workers)) === [] && $parents === [$parent],
                $pool === 'default'
                    ? 'the forked workers ran the jobs of default'
                    : 'the spawned workers ran the jobs of isolated',
            );
        }

        // A deploy: the workers stop after their job, and the master starts a
        // new fork server, so that the code on disk is booted again.
        $this->line("\nphp artisan queue:restart");
        $this->callSilently('queue:restart');
        $old = $tree;
        $tree = $this->waitFor('a new fork server and its workers', 30, function () use ($old) {
            $tree = $this->bootedTree();

            return $tree !== null
                && $tree['forkServer'] !== $old['forkServer']
                && array_intersect_key($tree['spawned'], $old['spawned']) === []
                && !Processes::alive($old['forkServer']) ? $tree : null;
        });
        $this->printTree($tree);
        $this->check(
            $tree['forkServer'] !== $old['forkServer'],
            "queue:restart brought a new fork server, pid {$tree['forkServer']}",
        );
        $this->check(count($tree['forked']) === 2, 'the workers forked since then are its children');
        $this->check(
            !Processes::alive($old['forkServer']),
            "the old fork server, pid {$old['forkServer']}, exited with its workers",
        );

        // Stop the supervisor the way a deploy does: it drains every worker,
        // then exits.
        $this->line("\nphp artisan queen:supervisor terminate");
        $everyPid = Processes::tree(Processes::all(), $this->master);
        $this->callSilently('queen:supervisor', ['action' => 'terminate']);
        $stopped = microtime(true);
        $this->waitFor('the master to exit', 40, fn () => !$this->isRunning($this->master));
        $this->line(sprintf('  the master exited after %.1f s', microtime(true) - $stopped));
        $this->check(
            array_filter($everyPid, fn (int $pid) => Processes::alive($pid)) === [],
            'no process of the tree is left',
        );
    }

    /**
     * The supervisor's processes by role, once both pools run 2 workers
     * that have booted (they left their mark: see AppServiceProvider);
     * null until then.
     *
     * @return array{all: array, forkServer: int, forked: array<int, true>, spawned: array<int, true>}|null
     */
    private function bootedTree(): ?array
    {
        $all = Processes::all();
        $tree = ['all' => $all, 'forkServer' => 0, 'forked' => [], 'spawned' => []];
        foreach ($all as $pid => $process) {
            if ($process['ppid'] === $this->master && str_contains($process['command'], 'queen:fork-server')) {
                $tree['forkServer'] = $pid;
            }
        }
        foreach ($all as $pid => $process) {
            if ($tree['forkServer'] !== 0 && $process['ppid'] === $tree['forkServer']) {
                $tree['forked'][$pid] = true;
            } elseif ($process['ppid'] === $this->master && str_contains($process['command'], 'queue:work')) {
                $tree['spawned'][$pid] = true;
            }
        }
        $booted = fn (array $workers, string $queue) => count($workers) >= 2
            && array_filter(
                array_keys($workers),
                fn (int $pid) => !is_file(Journal::readyPath($queue, $pid)),
            ) === [];

        return $booted($tree['forked'], $this->queues['default'])
            && $booted($tree['spawned'], $this->queues['isolated']) ? $tree : null;
    }

    private function printTree(array $tree): void
    {
        $this->line("\n  pid     parent  resident   role");
        $rows = [$this->master => 'master: queen:supervise', $tree['forkServer'] => 'fork server'];
        foreach (array_keys($tree['forked']) as $pid) {
            $rows[$pid] = 'forked worker, pool default';
        }
        foreach (array_keys($tree['spawned']) as $pid) {
            $rows[$pid] = 'spawned worker, pool isolated';
        }
        foreach ($rows as $pid => $role) {
            $process = $tree['all'][$pid];
            $mib = $process['rss_kib'] / 1024;
            $this->line(sprintf('  %-7d %-7d %5.1f MiB  %s', $pid, $process['ppid'], $mib, $role));
        }
    }

    /** Linux splits a worker's memory into what it shares and what is its own. */
    private function printMemory(array $tree): void
    {
        $workers = [
            'forked worker' => array_key_first($tree['forked']),
            'spawned worker' => array_key_first($tree['spawned']),
        ];
        if (Processes::smaps($workers['forked worker']) === null) {
            $this->line("\n  memory: resident sets only, this host has no /proc/<pid>/smaps_rollup");
            $this->line('  to split them into shared and private; the memory checks need Linux');

            return;
        }
        $this->line("\n  memory           resident   proportional   private");
        $memory = [];
        foreach ($workers as $role => $pid) {
            $m = $memory[$role] = array_map(fn (int $kib) => $kib / 1024, Processes::smaps($pid));
            $this->line(sprintf(
                '  %-15s %5.1f MiB   %5.1f MiB      %5.1f MiB',
                $role,
                $m['rss'],
                $m['pss'],
                $m['private'],
            ));
        }
        $forked = $memory['forked worker'];
        $this->check(
            $forked['private'] < $forked['rss'] / 2,
            'a forked worker\'s private memory is under half its resident set',
        );
        $this->check(
            $forked['private'] < $memory['spawned worker']['private'],
            'a forked worker has less private memory than a spawned one',
        );
    }
}
// docs:end
