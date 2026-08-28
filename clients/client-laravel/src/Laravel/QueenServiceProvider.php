<?php

namespace Queen\Laravel;

use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\ServiceProvider;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\SyncedFailedJobProvider;
use Queen\Laravel\Supervisor\WorkerTelemetry;
use Queen\Queen;

class QueenServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../../config/queen.php', 'queen');

        $this->registerDefaultQueueConnection();

        if ((bool) $this->app['config']->get('queen.sync_failed_jobs', true)) {
            $this->app->extend('queue.failer', function ($provider, $app) {
                return new SyncedFailedJobProvider(
                    $provider,
                    fn (string $connection) => $app['queue']->connection($connection),
                );
            });
        }

        $this->callAfterResolving(QueueManager::class, function (QueueManager $manager): void {
            $manager->addConnector('queen', fn () => new QueenConnector(
                $this->app['config']->get('queen', [])
            ));
        });

        $this->app->singleton(Queen::class, function ($app) {
            $config = $app['config']['queen'];
            $retry429 = $config['retry_429'] ?? [];
            if (is_array($retry429)) {
                // Unset env vars land here as nulls; drop them so the
                // per-request-kind defaults in Retry429Policy apply.
                $retry429 = array_filter($retry429, fn ($value) => $value !== null);
            }

            $queenConfig = [
                'bearerToken' => $config['bearer_token'],
                'timeoutMillis' => $config['timeout'],
                'retryAttempts' => $config['retry_attempts'],
                'retryDelayMillis' => $config['retry_delay'] ?? 1000,
                'loadBalancingStrategy' => $config['load_balancing_strategy'],
                'enableFailover' => $config['enable_failover'] ?? true,
                'affinityHashRing' => $config['affinity_hash_ring'] ?? 150,
                'healthRetryAfterMillis' => $config['health_retry_after'] ?? 30000,
                'headers' => $config['headers'] ?? [],
                'retry429' => $retry429,
            ];

            if (!empty($config['urls'])) {
                $queenConfig['urls'] = $config['urls'];
            } else {
                $queenConfig['url'] = $config['url'];
            }

            return new Queen($queenConfig);
        });

        $this->app->alias(Queen::class, 'queen');
    }

    private function registerDefaultQueueConnection(): void
    {
        $queen = $this->app['config']->get('queen', []);
        $defaults = [
            'driver' => 'queen',
            'url' => $queen['url'] ?? 'http://localhost:6632',
            'urls' => $queen['urls'] ?? null,
            'bearer_token' => $queen['bearer_token'] ?? null,
            'timeout' => $queen['timeout'] ?? 30000,
            'retry_attempts' => $queen['retry_attempts'] ?? 3,
            'retry_delay' => $queen['retry_delay'] ?? 1000,
            'load_balancing_strategy' => $queen['load_balancing_strategy'] ?? 'affinity',
            'enable_failover' => $queen['enable_failover'] ?? true,
            'affinity_hash_ring' => $queen['affinity_hash_ring'] ?? 150,
            'health_retry_after' => $queen['health_retry_after'] ?? 30000,
            'retry_429' => $queen['retry_429'] ?? [],
            'headers' => $queen['headers'] ?? [],
            'queue' => $queen['queue'] ?? 'default',
            'consumer_group' => $queen['consumer_group'] ?? 'laravel',
            'partitions' => $queen['partitions'] ?? 64,
            'partition_prefix' => $queen['partition_prefix'] ?? 'laravel',
            'retry_after' => $queen['retry_after'] ?? 90,
            'block_for' => $queen['block_for'] ?? 0,
            'prefetch' => $queen['prefetch'] ?? 1,
            'ack_batch' => $queen['ack_batch'] ?? 1,
            'bulk_batch' => $queen['bulk_batch'] ?? 100,
            'after_commit' => $queen['after_commit'] ?? false,
        ];
        $existing = $this->app['config']->get('queue.connections.queen', []);

        $this->app['config']->set('queue.connections.queen', array_replace(
            $defaults,
            is_array($existing) ? $existing : [],
        ));
    }

    public function boot(): void
    {
        $this->registerWorkerTelemetry();

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../../config/queen.php' => config_path('queen.php'),
            ], 'queen-config');

            $this->commands([
                Commands\ConsumeCommand::class,
                Commands\SuperviseCommand::class,
                Commands\SupervisorConfigCommand::class,
                Commands\SupervisorControlCommand::class,
            ]);
        }
    }

    private function registerWorkerTelemetry(): void
    {
        $directory = getenv('QUEEN_SUPERVISOR_TELEMETRY_DIR');
        if (!is_string($directory) || $directory === '') {
            return;
        }

        $connection = getenv('QUEEN_LARAVEL_CONNECTION');
        $supervisor = getenv('QUEEN_LARAVEL_SUPERVISOR');
        $group = getenv('QUEEN_LARAVEL_CONSUMER_GROUP');
        $telemetry = new WorkerTelemetry(
            $directory,
            is_string($connection) && $connection !== '' ? $connection : 'queen',
            is_string($supervisor) && $supervisor !== '' ? $supervisor : 'default',
            is_string($group) && $group !== '' ? $group : 'laravel',
        );
        $events = $this->app['events'];
        $events->listen(JobProcessing::class, fn (JobProcessing $event) => $telemetry->start($event->connectionName, $event->job));
        $events->listen(JobProcessed::class, fn (JobProcessed $event) => $telemetry->finish($event->connectionName, $event->job));
        $events->listen(JobExceptionOccurred::class, fn (JobExceptionOccurred $event) => $telemetry->finish($event->connectionName, $event->job, true));
        $events->listen(JobFailed::class, fn (JobFailed $event) => $telemetry->finish($event->connectionName, $event->job, true));
    }
}
