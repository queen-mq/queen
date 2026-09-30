<?php

namespace Queen\Laravel;

use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Queen\Laravel\Dashboard\DashboardRepository;
use Queen\Laravel\Dashboard\DashboardScript;
use Queen\Laravel\Dashboard\DashboardStylesheet;
use Queen\Laravel\Dashboard\FailedJobsReadModel;
use Queen\Laravel\Dashboard\RemoteStatusReader;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Laravel\Monitoring\QueueWaits;
use Queen\Laravel\Monitoring\TagMonitor;
use Illuminate\Queue\Events\JobFailed;
use Queen\Laravel\Queue\QueenQueue;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;
use Queen\Laravel\Dashboard\ThroughputReader;
use Queen\Laravel\Http\Middleware\AuthorizeDashboard;
use Queen\Laravel\Http\Middleware\SecureDashboardResponse;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\SyncedFailedJobProvider;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Laravel\Supervisor\SupervisorState;
use Queen\Laravel\Supervisor\WorkerTelemetry;
use Queen\Queen;
use RuntimeException;

class QueenServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../../config/queen.php', 'queen');

        $this->registerDefaultQueueConnection();
        $this->registerDashboardServices();

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
            'autopilot' => $queen['autopilot'] ?? false,
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

    private function registerDashboardServices(): void
    {
        $this->app->singleton(
            DashboardStylesheet::class,
            fn ($app): DashboardStylesheet => new DashboardStylesheet($app->publicPath()),
        );
        $this->app->singleton(
            DashboardScript::class,
            fn ($app): DashboardScript => new DashboardScript($app->publicPath()),
        );

        $this->app->singleton(FailedJobsReadModel::class, function ($app): FailedJobsReadModel {
            return new FailedJobsReadModel(
                $app['config'],
                function (?string $connection) use ($app): mixed {
                    if (!$app->bound('db')) {
                        throw new RuntimeException('Laravel database services are unavailable.');
                    }

                    return $app['db']->connection($connection);
                },
            );
        });

        $this->app->singleton(DashboardRepository::class, function ($app): DashboardRepository {
            $directory = SupervisorConfiguration::stateDirectory(
                $app['config']->get('queen.supervisor.state_directory'),
                $app->basePath(),
            );

            return new DashboardRepository(
                new SupervisorState($directory),
                $app['config'],
                fn (int $limit, ?int $cursor = null): array => $app->make(FailedJobsReadModel::class)->read($limit, $cursor),
                $this->remoteStatusEnabled($app)
                    ? fn (): ?array => $app->make(RemoteStatusReader::class)->read()
                    : null,
                fn (string $id): ?array => $app->make(FailedJobsReadModel::class)->find($id),
            );
        });

        $this->app->singleton(JobMetricsRecorder::class, function ($app): JobMetricsRecorder {
            $connection = (string) $app['config']->get('queen.job_metrics.connection', 'queen');

            return new JobMetricsRecorder(
                function () use ($app, $connection): ?Queen {
                    $queue = $app['queue']->connection($connection);

                    return $queue instanceof QueenQueue ? $queue->getQueen() : null;
                },
                (string) $app['config']->get('queen.job_metrics.namespace', 'queen-metrics'),
            );
        });

        $this->app->singleton(TagMonitor::class, function ($app): TagMonitor {
            $connection = (string) $app['config']->get('queen.tags.connection', 'queen');

            return new TagMonitor(
                function () use ($app, $connection): ?Queen {
                    $queue = $app['queue']->connection($connection);

                    return $queue instanceof QueenQueue ? $queue->getQueen() : null;
                },
                (string) $app['config']->get('queen.tags.namespace', 'queen-metrics'),
                max(60, (int) $app['config']->get('queen.tags.retention_minutes', 1440) * 60),
            );
        });

        $this->app->singleton(JobMetricsReader::class, function ($app): JobMetricsReader {
            $resolved = SupervisorConfiguration::readOnlyConnection(
                (string) $app['config']->get('queen.job_metrics.connection', 'queen'),
                (array) $app['config']->get('queen.supervisor', []),
                (array) $app['config']->get('queen', []),
                (array) $app['config']->get('queue.connections', []),
            );

            return new JobMetricsReader(
                new Queen([
                    'urls' => $resolved['urls'],
                    'bearerToken' => $resolved['bearer_token'],
                    'headers' => $resolved['headers'],
                    // A dashboard render must not queue behind retries.
                    'timeoutMillis' => 5000,
                    'retryAttempts' => 1,
                    'retryDelayMillis' => 0,
                ]),
                (string) $app['config']->get('queen.job_metrics.namespace', 'queen-metrics'),
                $app->bound('cache') ? fn () => $app['cache']->store() : null,
            );
        });

        $this->app->singleton(ThroughputReader::class, function ($app): ThroughputReader {
            // A dashboard render must not queue behind retries or a slow broker.
            $timeout = min(5, $this->configurationInteger(
                $app['config']->get('queen.supervisor.http_timeout', 5),
                'queen.supervisor.http_timeout',
                1,
            ));

            return new ThroughputReader(
                function (string $connection) use ($app, $timeout): Queen {
                    $resolved = SupervisorConfiguration::readOnlyConnection(
                        $connection,
                        (array) $app['config']->get('queen.supervisor', []),
                        (array) $app['config']->get('queen', []),
                        (array) $app['config']->get('queue.connections', []),
                    );

                    return new Queen([
                        'urls' => $resolved['urls'],
                        'bearerToken' => $resolved['bearer_token'],
                        'headers' => $resolved['headers'],
                        'timeoutMillis' => $timeout * 1000,
                        'retryAttempts' => 1,
                        'retryDelayMillis' => 0,
                    ]);
                },
                $app->bound('cache') ? fn () => $app['cache']->store() : null,
                null,
                $timeout * 1000,
            );
        });

        // Wait measurement for queen:check-waits, with the connection's
        // read credential; bound so tests and applications can replace it.
        $this->app->bind(QueueWaits::class, function ($app, array $parameters): QueueWaits {
            $resolved = SupervisorConfiguration::readOnlyConnection(
                (string) ($parameters['connection'] ?? 'queen'),
                (array) $app['config']->get('queen.supervisor', []),
                (array) $app['config']->get('queen', []),
                (array) $app['config']->get('queue.connections', []),
            );

            return new QueueWaits(new Queen([
                'urls' => $resolved['urls'],
                'bearerToken' => $resolved['bearer_token'],
                'headers' => $resolved['headers'],
                'timeoutMillis' => 5000,
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
            ]));
        });

        $this->app->singleton(RemoteStatusReader::class, function ($app): RemoteStatusReader {
            $settings = SupervisorConfiguration::remoteStatusSettings(
                (array) $app['config']->get('queen.supervisor', []),
                (array) $app['config']->get('queen', []),
                (array) $app['config']->get('queue.connections', []),
            );
            if ($settings === null) {
                throw new RuntimeException('Queen supervisor remote status is disabled.');
            }
            $connection = $settings['connection'];
            $timeout = $this->configurationInteger(
                $app['config']->get('queen.supervisor.http_timeout', 5),
                'queen.supervisor.http_timeout',
                1,
            );

            return new RemoteStatusReader(
                new Queen([
                    'urls' => $connection['urls'],
                    'bearerToken' => $connection['bearer_token'],
                    'headers' => $connection['headers'],
                    'timeoutMillis' => $timeout * 1000,
                    // A dashboard render must not queue behind retries.
                    'retryAttempts' => 1,
                    'retryDelayMillis' => 0,
                ]),
                $settings['namespace'],
                $settings['key'],
            );
        });
    }

    private function remoteStatusEnabled($app): bool
    {
        return $app['config']->get('queen.supervisor.remote_status.enabled', false) === true;
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
        $this->loadViewsFrom(__DIR__ . '/../../resources/views', 'queen');
        $this->registerDashboardRoutes();
        $this->registerMetricsRoute();

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../../config/queen.php' => config_path('queen.php'),
            ], 'queen-config');
            // Optional: only for web servers that serve every *.css or *.js
            // from the public directory. The dashboard falls back to its own
            // routes whenever a copy is missing or stale. `laravel-assets` makes
            // the default skeleton's post-update-cmd republish them on upgrade.
            $this->publishes([
                __DIR__ . '/../../resources/css/dashboard.css' => public_path(DashboardStylesheet::PUBLISHED_FILE),
                __DIR__ . '/../../resources/js/dashboard.js' => public_path(DashboardScript::PUBLISHED_FILE),
            ], ['queen-assets', 'laravel-assets']);

            $this->commands([
                Commands\ConsumeCommand::class,
                Commands\SuperviseCommand::class,
                Commands\SupervisorConfigCommand::class,
                Commands\SupervisorControlCommand::class,
                Commands\ForkServerCommand::class,
                Commands\CheckWaitsCommand::class,
                Commands\SupervisorInstallCommand::class,
            ]);
        }
    }

    private function registerDashboardRoutes(): void
    {
        if ($this->app['config']->get('queen.dashboard.enabled', false) !== true
            || $this->app->routesAreCached()) {
            return;
        }

        $path = $this->app['config']->get('queen.dashboard.path', 'queen');
        if (!is_string($path)
            || trim($path, '/') === ''
            || strlen($path) > 128
            || preg_match('/\A[A-Za-z0-9][A-Za-z0-9._~\/-]*\z/D', trim($path, '/')) !== 1
            || in_array('..', explode('/', trim($path, '/')), true)) {
            throw new \InvalidArgumentException('queen.dashboard.path must be a safe non-empty route prefix.');
        }

        $configuredMiddleware = $this->app['config']->get('queen.dashboard.middleware', []);
        if (!is_array($configuredMiddleware)) {
            throw new \InvalidArgumentException('queen.dashboard.middleware must be an array of middleware names.');
        }
        $middleware = ['web', SecureDashboardResponse::class];
        foreach ($configuredMiddleware as $entry) {
            if (!is_string($entry)
                || $entry === ''
                || strlen($entry) > 256
                || preg_match('/[\x00-\x1F\x7F]/', $entry) === 1) {
                throw new \InvalidArgumentException('queen.dashboard.middleware contains an invalid middleware name.');
            }
            if ($entry !== 'web') {
                $middleware[] = $entry;
            }
        }
        // Application authentication must resolve the user before the Queen
        // Gate runs. The security-response wrapper remains outside both.
        $middleware[] = AuthorizeDashboard::class;

        $attributes = [
            'prefix' => trim($path, '/'),
            'as' => 'queen.dashboard.',
            'middleware' => array_values(array_unique($middleware)),
        ];
        $domain = $this->app['config']->get('queen.dashboard.domain');
        if ($domain !== null) {
            if (!is_string($domain)
                || $domain === ''
                || strlen($domain) > 253
                || preg_match('/\A[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\z/D', $domain) !== 1) {
                throw new \InvalidArgumentException('queen.dashboard.domain must be a safe hostname pattern.');
            }
            $attributes['domain'] = $domain;
        }

        $this->app['router']->group($attributes, function (): void {
            require __DIR__ . '/../../routes/dashboard.php';
        });
    }

    private function registerMetricsRoute(): void
    {
        if ($this->app['config']->get('queen.metrics.enabled', false) !== true
            || $this->app->routesAreCached()) {
            return;
        }

        $token = $this->app['config']->get('queen.metrics.token');
        if (!is_string($token) || strlen($token) < 32) {
            throw new \InvalidArgumentException('queen.metrics.token must be a secret of at least 32 characters.');
        }
        $path = $this->app['config']->get('queen.metrics.path', 'queen/metrics');
        if (!is_string($path)
            || trim($path, '/') === ''
            || strlen($path) > 128
            || preg_match('/\A[A-Za-z0-9][A-Za-z0-9._~\/-]*\z/D', trim($path, '/')) !== 1
            || in_array('..', explode('/', trim($path, '/')), true)) {
            throw new \InvalidArgumentException('queen.metrics.path must be a safe non-empty route path.');
        }

        $this->app['router']
            ->get(trim($path, '/'), Http\Controllers\MetricsController::class)
            ->middleware(Http\Middleware\AuthorizeMetrics::class)
            ->name('queen.metrics');
    }

    private function registerWorkerTelemetry(): void
    {
        WorkerTelemetry::listenFromEnvironment($this->app['events']);
        $this->registerJobMetrics();
    }

    /**
     * Per-job-class metrics for the dashboard's Jobs page, recorded by the
     * workers of Queen connections; see Monitoring\JobMetricsRecorder.
     */
    private function registerJobMetrics(): void
    {
        if ($this->app['config']->get('queen.job_metrics.enabled', true) !== true) {
            return;
        }
        $isQueen = fn (?string $connection): bool => $connection !== null
            && $this->app['config']->get("queue.connections.{$connection}.driver") === 'queen';
        $recorder = fn (): JobMetricsRecorder => $this->app->make(JobMetricsRecorder::class);
        $events = $this->app['events'];
        $events->listen(JobProcessing::class, function (JobProcessing $event) use ($isQueen, $recorder): void {
            if ($isQueen($event->connectionName)) {
                $recorder()->start($event->job);
            }
        });
        $events->listen(JobProcessed::class, function (JobProcessed $event) use ($isQueen, $recorder): void {
            if ($isQueen($event->connectionName)) {
                $recorder()->finish($event->job, false);
            }
        });
        // A final failure raises JobExceptionOccurred before JobFailed; count it once.
        $events->listen(JobExceptionOccurred::class, function (JobExceptionOccurred $event) use ($isQueen, $recorder): void {
            if ($isQueen($event->connectionName)) {
                $recorder()->finish($event->job, true);
            }
        });
        $events->listen(Looping::class, function (Looping $event) use ($isQueen, $recorder): void {
            if ($isQueen($event->connectionName)) {
                $recorder()->tick();
            }
        });
        $events->listen(WorkerStopping::class, fn () => $recorder()->flush());

        if ($this->app['config']->get('queen.tags.enabled', true) !== true) {
            return;
        }
        $tags = fn (): TagMonitor => $this->app->make(TagMonitor::class);
        $events->listen(JobProcessing::class, function (JobProcessing $event) use ($isQueen, $tags): void {
            if ($isQueen($event->connectionName)) {
                $tags()->start($event->job);
            }
        });
        $events->listen(JobProcessed::class, function (JobProcessed $event) use ($isQueen, $tags): void {
            if ($isQueen($event->connectionName)) {
                $tags()->record($event->job, 'completed');
            }
        });
        $events->listen(JobFailed::class, function (JobFailed $event) use ($isQueen, $tags): void {
            if ($isQueen($event->connectionName)) {
                $tags()->record($event->job, 'failed');
            }
        });
    }
}
