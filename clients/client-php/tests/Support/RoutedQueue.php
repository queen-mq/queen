<?php

namespace Queen\Tests\Support;

use Illuminate\Contracts\Queue\Factory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use LogicException;

/**
 * A `routed` connection, an application's queue.default that only
 * dispatches: to the connection of the pool its queue belongs to, under
 * that pool's queue name, and leaves payloads, events and after_commit to
 * that connection. Workers pop from the pools' connections; a pop here
 * throws.
 */
final class RoutedQueue extends Queue implements QueueContract
{
    /** The pool of each queue of these tests. */
    public const POOLS = [
        'default' => 'interactive',
        'notifications' => 'interactive',
        'compliance' => 'batch',
        'ical' => 'batch',
        'background' => 'batch',
        'reservation-sync' => 'ordered',
    ];

    /** Pops asked of any router: a pool's worker never should. */
    public static int $pops = 0;

    public function __construct(private Factory $queues, private string $environment = 'testing')
    {
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
        self::$pops++;

        throw new LogicException('The routed queue connection only dispatches; workers pop from the pool connections.');
    }

    /** @return array{0: QueueContract, 1: string} */
    public function route(?string $queue): array
    {
        $name = $queue ?: 'default';
        $pool = self::POOLS[$name] ?? null;
        if ($pool === null) {
            throw new LogicException("No pool for queue [{$name}].");
        }

        return [$this->queues->connection('queen-' . $pool), "app.{$this->environment}.{$name}"];
    }
}
