<?php

namespace App\Queue;

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\Connectors\ConnectorInterface;

/** The `routed` queue driver: see RoutedQueue. */
final class RoutedConnector implements ConnectorInterface
{
    public function __construct(private readonly Application $app)
    {
    }

    public function connect(array $config): RoutedQueue
    {
        return new RoutedQueue(
            $this->app['queue'],
            QueueRoutes::fromConfig(),
            $this->app['log'],
            (string) ($config['fallback'] ?? config('benchmark.connection')),
        );
    }
}
