<?php

namespace App\Queue;

use Illuminate\Contracts\Queue\Factory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use LogicException;
use Psr\Log\LoggerInterface;

/**
 * A default queue connection that only dispatches, as an application that
 * moves its queues one pool at a time may have. Each push, later and bulk goes to the connection of the pool
 * that owns the queue, `queen-<pool>` or `redis-<pool>`, under that backend's
 * queue name. It touches no payload, event or after_commit: the target
 * connection does all of that. Workers pop from the pool connections, never
 * from this one.
 */
final class RoutedQueue extends Queue implements QueueContract
{
    public const CONNECTION = 'routed';

    public function __construct(
        private readonly Factory $queues,
        private readonly QueueRoutes $routes,
        private readonly LoggerInterface $logger,
        private readonly string $fallbackConnection,
    ) {
    }

    public function size($queue = null)
    {
        [$target, $name] = $this->route($queue);

        return $target->size($name);
    }

    public function push($job, $data = '', $queue = null)
    {
        [$target, $name] = $this->route($queue);

        return $target->push($job, $data, $name);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        [$target, $name] = $this->route($queue);

        return $target->pushRaw($payload, $name, $options);
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        [$target, $name] = $this->route($queue);

        return $target->later($delay, $job, $data, $name);
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
        [$target, $name] = $this->route($queue);
        $target->bulk($jobs, $data, $name);
    }

    public function pop($queue = null)
    {
        throw new LogicException('The routed queue connection only dispatches; workers pop from the pool connections.');
    }

    /** @return array{0: QueueContract, 1: string} the target connection and its queue name */
    public function route(?string $queue): array
    {
        $name = $queue ?: 'default';
        $pool = $this->routes->poolFor($name);
        if ($pool === null) {
            $this->logger->warning('Dispatch to an unknown queue', ['queue' => $name]);

            return [$this->queues->connection($this->fallbackConnection), $name];
        }
        if ($this->routes->backendFor($name) === 'queen') {
            return [$this->queues->connection('queen-' . $pool), QueueRoutes::queenName($name)];
        }

        return [$this->queues->connection('redis-' . $pool), $name];
    }
}
