<?php

namespace Queen\Laravel;

use Illuminate\Contracts\Cache\LockTimeoutException;
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
use RuntimeException;

class QueenServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../../config/queen.php', 'queen');

        $this->registerDefaultQueueConnection();

        if ((bool) $this->app['config']->get('queen.sync_failed_jobs', true)) {
            $this->app->extend('queue.failer', function ($provider, $app) {
                $lockStore = $this->optionalString(
                    $app['config']->get('queen.failed_jobs_lock_store'),
                    'queen.failed_jobs_lock_store',
                );
                $lockName = $this->requiredString(
                    $app['config']->get('queen.failed_jobs_lock_name', 'queen:failed-jobs'),
                    'queen.failed_jobs_lock_name',
                );
                $lockTtl = $this->configurationInteger(
                    $app['config']->get('queen.failed_jobs_lock_ttl', 600),
                    'queen.failed_jobs_lock_ttl',
                    1,
                );
                $lockWait = $this->configurationInteger(
                    $app['config']->get('queen.failed_jobs_lock_wait', 600),
                    'queen.failed_jobs_lock_wait',
                    0,
                );

                return new SyncedFailedJobProvider(
                    $provider,
                    fn (string $connection) => $app['queue']->connection($connection),
                    function (\Closure $operation) use ($app, $lockStore, $lockName, $lockTtl, $lockWait): mixed {
                        $cache = $lockStore === null ? $app['cache'] : $app['cache']->store($lockStore);
                        $lock = $cache->lock($lockName, $lockTtl);
                        if (!method_exists($lock, 'isOwnedByCurrentProcess')) {
                            throw new RuntimeException(
                                'The configured Queen failed-job cache lock cannot verify ownership.',
                            );
                        }

                        try {
                            return $lock->block($lockWait, function () use ($lock, $operation): mixed {
                                $assertOwned = static function () use ($lock): void {
                                    if (!$lock->isOwnedByCurrentProcess()) {
                                        throw new RuntimeException(
                                            'The Queen failed-job cache lock expired during a mutation.',
                                        );
                                    }
                                };

                                $assertOwned();
                                $result = $operation($assertOwned);
                                $assertOwned();

                                return $result;
                            });
                        } catch (LockTimeoutException $exception) {
                            throw new RuntimeException(
                                'Timed out acquiring the Queen failed-job cache lock; no index mutation was attempted.',
                                previous: $exception,
                            );
                        }
                    },
                );
            });
        }

        $this->callAfterResolving(QueueManager::class, function (QueueManager $manager): void {
            $manager->addConnector('queen', function (): QueenConnector {
                $retryHandler = null;
                if ((bool) $this->app['config']->get('queen.sync_failed_jobs', true)) {
                    $retryHandler = function (string $fence, \Closure $republish): mixed {
                        $provider = $this->app['queue.failer'];
                        if (!$provider instanceof SyncedFailedJobProvider) {
                            throw new RuntimeException(
                                'Queen failed-job synchronization is enabled, but its synchronized provider is unavailable.',
                            );
                        }

                        return $provider->retryWithFence($fence, $republish);
                    };
                }

                return new QueenConnector(
                    $this->app['config']->get('queen', []),
                    $retryHandler,
                );
            });
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
            'lease_renewal' => $queen['lease_renewal'] ?? false,
            'lease_renewal_interval' => $queen['lease_renewal_interval'] ?? null,
            'lease_renewal_timeout' => $queen['lease_renewal_timeout'] ?? 5,
            'lease_renewal_kill_grace' => $queen['lease_renewal_kill_grace'] ?? 2,
            'lease_renewal_safety_margin' => $queen['lease_renewal_safety_margin'] ?? 1,
            'bulk_batch' => $queen['bulk_batch'] ?? 100,
            'after_commit' => $queen['after_commit'] ?? false,
        ];
        $existing = $this->app['config']->get('queue.connections.queen', []);

        $this->app['config']->set('queue.connections.queen', array_replace(
            $defaults,
            is_array($existing) ? $existing : [],
        ));
    }

    private function configurationInteger(mixed $value, string $name, int $minimum): int
    {
        $integer = false;
        if (is_int($value)) {
            $integer = $value;
        } elseif (is_string($value) && preg_match('/^[0-9]+$/D', $value) === 1) {
            $digits = ltrim($value, '0');
            $integer = filter_var($digits === '' ? '0' : $digits, FILTER_VALIDATE_INT);
        }

        if ($integer === false || $integer < $minimum) {
            throw new \InvalidArgumentException("{$name} must be an integer of at least {$minimum}.");
        }

        return $integer;
    }

    private function requiredString(mixed $value, string $name): string
    {
        if (!is_string($value) || trim($value) === '' || preg_match('/[\x00-\x1F\x7F]/', $value) === 1) {
            throw new \InvalidArgumentException("{$name} must be a non-empty string without control characters.");
        }

        return $value;
    }

    private function optionalString(mixed $value, string $name): ?string
    {
        return $value === null ? null : $this->requiredString($value, $name);
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
                Commands\SupervisorInstallCommand::class,
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
