<?php

namespace App\Queue;

/**
 * Which pool owns each queue, and which backend serves it. An application may
 * choose the backend per queue at run time, to move a queue from Redis to
 * Queen while it runs; a lane here has one backend for every queue.
 */
final class QueueRoutes
{
    /**
     * @param array<string, string> $pools pool per Laravel queue name
     * @param string $backend `queen` or `redis`
     */
    public function __construct(private readonly array $pools, private readonly string $backend)
    {
    }

    public static function fromConfig(): self
    {
        $pools = [];
        foreach ((array) config('benchmark.routed_pools') as $pool => $settings) {
            foreach ($settings['queues'] as $queue) {
                $pools[$queue] = (string) $pool;
            }
        }

        return new self($pools, (string) config('benchmark.connection'));
    }

    public function poolFor(string $queue): ?string
    {
        return $this->pools[$queue] ?? null;
    }

    public function backendFor(string $queue): string
    {
        return $this->backend;
    }

    /** The Queen queue that a Laravel queue name maps to: `app.<env>.<queue>`. */
    public static function queenName(string $queue): string
    {
        return 'app.' . config('app.env') . '.' . $queue;
    }
}
