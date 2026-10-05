<?php

namespace Queen\Laravel\Queue;

/**
 * prefetch "auto": the size of a worker's next pop, from how long its jobs
 * take.
 *
 * A pop costs a round trip to the broker, and a prefetched batch holds jobs
 * other workers could run. Short jobs repay a larger batch; long ones do not.
 * So each queue's next batch aims at TARGET_MILLIS of work: the time a job
 * takes, from the moment pop() hands it to Laravel to the worker's next
 * pop(), its ACK included, averaged with a moving weight. An empty long poll
 * and the worker's sleep fall between a pop() and the next job, so they never
 * count as a job.
 *
 * The batch only grows after a pop that came back full, and at most twofold.
 * A pop that comes back short means the queue had no backlog: a larger batch
 * would only hold jobs that idle workers could run, behind the job running
 * here, so the next pop asks for what the queue had, one job after an empty
 * pop. Under a backlog every pop comes back full and the batch reaches its
 * target within a few pops.
 */
final class AdaptiveBatch
{
    /** The largest batch "auto" asks for. */
    public const CEILING = 16;

    /** About this much work per batch, in milliseconds. */
    public const TARGET_MILLIS = 250;

    /** The share of the newest job in the moving average. */
    private const WEIGHT = 0.3;

    /** @var array<string, float> moving average of a job, per queue */
    private array $jobMillis = [];

    /** @var array<string, int> the size last asked for, per queue */
    private array $size = [];

    /** @var array<string, bool> whether the queue's last pop came back full */
    private array $full = [];

    /** @var array{0: string, 1: float}|null the job handed out and when */
    private ?array $running = null;

    private \Closure $clock;

    /** Whether a configured prefetch asks for this: the word "auto". */
    public static function isAuto(mixed $prefetch): bool
    {
        return is_string($prefetch) && strtolower(trim($prefetch)) === 'auto';
    }

    /** @param (\Closure(): float)|null $clock monotonic milliseconds */
    public function __construct(
        private int $ceiling = self::CEILING,
        private int $targetMillis = self::TARGET_MILLIS,
        ?\Closure $clock = null,
    ) {
        $this->clock = $clock ?? static fn (): float => hrtime(true) / 1e6;
    }

    /** pop() handed a job of $queue to Laravel. */
    public function handedOut(string $queue): void
    {
        $this->running = [$queue, ($this->clock)()];
    }

    /** pop() was called: the job handed out before it has ended. */
    public function popping(): void
    {
        if ($this->running === null) {
            return;
        }
        [$queue, $since] = $this->running;
        $this->running = null;
        $millis = max(0.0, ($this->clock)() - $since);
        $average = $this->jobMillis[$queue] ?? null;
        $this->jobMillis[$queue] = $average === null ? $millis : $average + self::WEIGHT * ($millis - $average);
    }

    /** A pop of $queue asked for $requested jobs and got $received. */
    public function popped(string $queue, int $requested, int $received): void
    {
        $this->full[$queue] = $received >= $requested;
        if ($received < $requested) {
            $this->size[$queue] = max(1, $received);
        }
    }

    /** The number of jobs the next pop of $queue asks for. */
    public function size(string $queue): int
    {
        $current = $this->size[$queue] ?? 1;
        $average = $this->jobMillis[$queue] ?? null;
        if ($average === null) {
            return $current;
        }
        $wanted = (int) max(1, min($this->ceiling, floor($this->targetMillis / max($average, 0.5))));
        $grown = ($this->full[$queue] ?? true) ? $current * 2 : $current;

        return $this->size[$queue] = min($wanted, $grown);
    }
}
