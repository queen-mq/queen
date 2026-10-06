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
 * The batch doubles only after two pops in a row came back full: one full pop
 * can be a short burst on a quiet queue, and a worker that grew its batch on
 * it would hold jobs that idle workers could run, behind the job running
 * here. A pop that comes back short means the queue had no backlog, so the
 * next pop asks for what the queue had, one job after an empty pop. Under a
 * backlog every pop comes back full: the first two ask for one job, the first
 * before any job was timed, then 2, 2, 4, 4, 8, 8, so the batch reaches 16 on
 * the ninth pop at the earliest, or stops lower when 250 ms holds fewer jobs.
 */
final class AdaptiveBatch
{
    /** The largest batch "auto" asks for. */
    public const CEILING = 16;

    /** About this much work per batch, in milliseconds. */
    public const TARGET_MILLIS = 250;

    /** Full pops in a row before the batch doubles. */
    private const FULL_POPS_TO_GROW = 2;

    /** The share of the newest job in the moving average. */
    private const WEIGHT = 0.3;

    /** @var array<string, float> moving average of a job, per queue */
    private array $jobMillis = [];

    /** @var array<string, int> the size last asked for, per queue */
    private array $size = [];

    /** @var array<string, int> full pops in a row since the batch last grew or a pop came back short */
    private array $fullPops = [];

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
        if ($received >= $requested) {
            $this->fullPops[$queue] = ($this->fullPops[$queue] ?? 0) + 1;

            return;
        }
        $this->fullPops[$queue] = 0;
        $this->size[$queue] = max(1, $received);
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
        $grown = $current;
        if (($this->fullPops[$queue] ?? 0) >= self::FULL_POPS_TO_GROW && $current < $wanted) {
            $grown = $current * 2;
            $this->fullPops[$queue] = 0;
        }

        return $this->size[$queue] = min($wanted, $grown);
    }
}
