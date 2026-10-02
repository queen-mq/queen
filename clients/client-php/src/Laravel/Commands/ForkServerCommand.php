<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Queue\Console\WorkCommand;
use Queen\Laravel\Supervisor\Prefork\ForkServer;
use Queen\Laravel\Supervisor\WorkerExitMarker;
use Queen\Laravel\Supervisor\WorkerTelemetry;
use Symfony\Component\Console\Command\Command as SymfonyCommand;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Started by a Queen supervisor master with prefork enabled; see ForkServer.
 */
class ForkServerCommand extends Command
{
    protected $signature = 'queen:fork-server';

    protected $description = 'Fork preforked queue workers for a Queen supervisor (internal)';

    protected $hidden = true;

    public function handle(): int
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_setsid')) {
            $this->error('queen:fork-server requires ext-pcntl and ext-posix.');

            return self::FAILURE;
        }
        // fd 3 alone proves nothing: a shell may have it open.
        if (getenv('QUEEN_FORK_SERVER') !== ForkServer::PROTOCOL) {
            $this->error('queen:fork-server is started by a Queen supervisor with prefork enabled; it is not run by hand.');

            return self::FAILURE;
        }
        $events = @fopen('php://fd/3', 'wb');
        if (!is_resource($events)) {
            $this->error('queen:fork-server must be started by a Queen supervisor, with fd 3 open.');

            return self::FAILURE;
        }

        // Load what every worker needs before the first fork, so all of them
        // share it; resolving the worker opens no connection.
        class_exists(WorkCommand::class);
        $this->laravel->make('queue.worker');
        $work = $this->getApplication()->find('queue:work');

        return (new ForkServer(
            STDIN,
            $events,
            fn (array $argv): int => $this->runWorker($work, $argv),
            fn () => $this->forgetInheritedConnections(),
        ))->serve();
    }

    /**
     * Run queue:work in a forked child, with the verbosity its arguments ask
     * for. Symfony applies --quiet and -v in Application::run(), which a
     * forked worker never goes through: without this, a pool configured
     * quiet printed two lines for every job.
     *
     * @param list<string> $argv
     */
    private function runWorker(SymfonyCommand $work, array $argv): int
    {
        $input = new ArgvInput(['artisan', ...$argv]);
        $verbosity = match (true) {
            $input->hasParameterOption(['--quiet', '-q'], true) => OutputInterface::VERBOSITY_QUIET,
            $input->hasParameterOption('-vvv', true) => OutputInterface::VERBOSITY_DEBUG,
            $input->hasParameterOption('-vv', true) => OutputInterface::VERBOSITY_VERY_VERBOSE,
            $input->hasParameterOption(['-v', '--verbose'], true) => OutputInterface::VERBOSITY_VERBOSE,
            default => null,
        };
        if ($verbosity !== null) {
            // The child's own copy of the server's output.
            $this->output->setVerbosity($verbosity);
        }

        return $work->run($input, $this->output);
    }

    /**
     * A provider may have opened a connection while the server booted; a
     * socket shared by two processes corrupts both conversations.
     */
    private function forgetInheritedConnections(): void
    {
        if ($this->laravel->resolved('db')) {
            foreach (array_keys($this->laravel['db']->getConnections()) as $name) {
                $this->laravel['db']->purge($name);
            }
        }
        if ($this->laravel->resolved('redis')) {
            foreach (array_keys($this->laravel['redis']->connections() ?? []) as $name) {
                $this->laravel['redis']->purge($name);
            }
        }
        WorkerTelemetry::listenFromEnvironment($this->laravel['events']);
        WorkerExitMarker::listenFromEnvironment($this->laravel['events']);
    }
}
