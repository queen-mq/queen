<?php

namespace Queen\Tests\Support;

use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;

/**
 * Laravel's Worker, for a given number of loops, without the second of sleep
 * it takes after a failed or an empty pop.
 */
final class ScriptedWorker extends Worker
{
    public int $maxLoops = 1;

    public int $loops = 0;

    /** @var (\Closure(int): void)|null before each loop's Looping */
    public ?\Closure $onLoop = null;

    protected function daemonShouldRun(WorkerOptions $options, $connectionName, $queue)
    {
        if ($this->loops >= $this->maxLoops) {
            $this->shouldQuit = true;

            return false;
        }
        $this->loops++;
        if ($this->onLoop !== null) {
            ($this->onLoop)($this->loops);
        }

        return parent::daemonShouldRun($options, $connectionName, $queue);
    }

    public function sleep($seconds)
    {
    }
}
