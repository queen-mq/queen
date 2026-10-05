<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Queue\Console\WorkCommand;
use Queen\Laravel\Supervisor\Prefork\ForkSafety;
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

    /** How long a thread left by the boot gets to end, see ForkSafety. */
    private const THREAD_GRACE_SECONDS = 5.0;

    public function handle(?ForkSafety $safety = null): int
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

        // Nothing the boot opened may reach the children, and a server with
        // a second thread is not forked at all: the master spawns instead.
        $this->releaseBootResources();
        $safety ??= new ForkSafety();
        $threads = $safety->threadsThatStay(self::THREAD_GRACE_SECONDS);
        if ($threads !== null && $threads !== []) {
            $this->error(sprintf(
                'queen:fork-server: the booted application runs %d more thread(s) (%s), which a forked worker would not have; '
                . 'prefork stays off and the workers are spawned. Find the extension or provider that starts them, or turn prefork off.',
                count($threads),
                implode(', ', $threads),
            ));

            return self::FAILURE;
        }
        $sockets = $safety->sockets();
        if ($sockets !== []) {
            $this->warn(sprintf(
                'queen:fork-server: %d socket(s) the boot opened stay open (fd %s), and every forked worker shares them. '
                . 'Open such a connection lazily, when a job first needs it.',
                count($sockets),
                implode(', ', $sockets),
            ));
        }

        return (new ForkServer(
            STDIN,
            $events,
            fn (array $argv): int => $this->runWorker($work, $argv),
            fn () => $this->prepareChild(),
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

    /** Runs in each forked child, before queue:work. */
    private function prepareChild(): void
    {
        $this->releaseBootResources();
        WorkerTelemetry::listenFromEnvironment($this->laravel['events']);
        WorkerExitMarker::listenFromEnvironment($this->laravel['events']);
    }

    /**
     * A provider may have opened a connection while the server booted; a
     * socket shared by two processes corrupts both conversations. Database
     * and Redis connections reconnect on their next use; log channels and
     * mailers are built again when next used.
     */
    private function releaseBootResources(): void
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
        if ($this->laravel->resolved('log')) {
            foreach (array_keys($this->laravel['log']->getChannels()) as $name) {
                $this->laravel['log']->forgetChannel($name);
            }
        }
        if ($this->laravel->resolved('mail.manager')) {
            $this->laravel['mail.manager']->forgetMailers();
        }
    }
}
