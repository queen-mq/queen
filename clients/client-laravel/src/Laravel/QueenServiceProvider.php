<?php

namespace Queen\Laravel;

use Illuminate\Support\ServiceProvider;
use Queen\Queen;

class QueenServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../../config/queen.php', 'queen');

        $this->app->singleton(Queen::class, function ($app) {
            $config = $app['config']['queen'];

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
                // Unset env vars land here as nulls; drop them so the
                // per-request-kind defaults in Retry429Policy apply.
                'retry429' => array_filter($config['retry_429'] ?? [], fn($value) => $value !== null),
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

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../../config/queen.php' => config_path('queen.php'),
            ], 'queen-config');

            $this->commands([
                Commands\ConsumeCommand::class,
            ]);
        }
    }
}
