<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Queen\Laravel\Supervisor\SupervisorState;

class SupervisorControlCommand extends Command
{
    protected $signature = 'queen:supervisor
        {action=status : status, pause, continue or terminate}
        {--json : Emit machine-readable status}
        {--check : Fail unless the supervisor is live and healthy}';
    protected $description = 'Inspect or control the local Queen worker supervisor';

    public function handle(): int
    {
        $configuredDirectory = $this->laravel['config']->get('queen.supervisor.state_directory');
        $state = new SupervisorState(is_string($configuredDirectory) && $configuredDirectory !== ''
            ? $configuredDirectory
            : $this->laravel->basePath('storage/queen-supervisor'));
        $action = (string) $this->argument('action');

        if ($action === 'status') {
            $status = $state->status();
            if ($status === null) {
                $this->components->warn('No Queen supervisor status is available.');
                return self::FAILURE;
            }
            $status['live'] = $state->isOwned();
            if (!$status['live'] && in_array($status['state'] ?? null, ['running', 'paused'], true)) {
                $status['state'] = 'stale';
            }
            if ($this->option('json')) {
                $this->line(json_encode($status, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
            } else {
                $this->table(['engine', 'state', 'live', 'pid', 'updated'], [[
                    $status['engine'] ?? '?', $status['state'] ?? '?',
                    $status['live'] ? 'yes' : 'no', $status['pid'] ?? '?', $status['updated_at'] ?? '?',
                ]]);
            }
            if ($this->option('check') && (!$status['live'] || !in_array($status['state'], ['running', 'paused'], true))) {
                return self::FAILURE;
            }
            return self::SUCCESS;
        }

        if (!in_array($action, ['pause', 'continue', 'terminate'], true)) {
            $this->components->error("Unknown action [{$action}].");
            return self::INVALID;
        }
        if (!$state->isOwned()) {
            $this->components->error('No live Queen supervisor owns this state directory.');
            return self::FAILURE;
        }
        $state->request($action);
        $this->components->info("Queen supervisor command [{$action}] requested.");
        return self::SUCCESS;
    }
}
