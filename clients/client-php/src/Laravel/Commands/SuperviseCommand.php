<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Queue\QueueManager;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;

class SuperviseCommand extends Command
{
    protected $signature = 'queen:supervise {--once : Reconcile once, then stop all children}';
    protected $description = 'Run the lightweight PHP process supervisor for Laravel workers';

    public function handle(QueueManager $queues): int
    {
        $config = SupervisorConfiguration::resolve(
            $this->laravel['config']->get('queen', []),
            $this->laravel->basePath(),
            queueConnections: $this->laravel['config']->get('queue.connections', []),
        );

        $this->components->info('Queen PHP supervisor started');
        $supervisor = new PhpSupervisor(
            $queues,
            $config,
            output: function (string $buffer, string $type): void {
                $type === 'err' ? $this->output->write("<fg=red>{$buffer}</>") : $this->output->write($buffer);
            },
        );
        $supervisor->run((bool) $this->option('once'));

        return self::SUCCESS;
    }
}
