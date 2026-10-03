<?php

namespace Queen\Laravel\Supervisor;

final class AutoScaler
{
    /**
     * The workers one supervisor should run per queue.
     *
     * Coordinated replicas of the same pool each pass their position among
     * the live replicas. The backlog then sizes one fleet target, each
     * replica takes an even share of it (the remainder going to the lowest
     * positions, and at least its slice of the queues with backlog), and
     * min_processes and max_processes bound every replica.
     * Fixed pools are not split: every replica runs its `processes`.
     *
     * @param int $replica this supervisor's position among the replicas, from 0
     * @param int $replicas the live replicas of this pool, this one included
     * @return array<string, int>
     */
    public function desired(array $options, array $depths, array $runtimes = [], int $replica = 0, int $replicas = 1): array
    {
        if ($replicas < 1 || $replica < 0 || $replica >= $replicas) {
            throw new \InvalidArgumentException("Replica [{$replica}] of [{$replicas}] is not a valid position.");
        }
        $queues = $options['queues'];
        $allocation = array_fill_keys($queues, 0);
        $depths = array_replace($allocation, array_map(fn ($value) => max(0, (int) $value), $depths));

        if ($options['balance'] === 'simple') {
            return $this->spread($allocation, (int) $options['processes'], array_fill_keys($queues, 1));
        }

        $weights = $depths;
        $nonFinitePressure = false;
        if (($options['strategy'] ?? 'size') === 'time') {
            foreach ($weights as $queue => $depth) {
                $runtime = $runtimes[$queue] ?? $options['default_runtime_seconds'];
                $runtime = is_numeric($runtime) ? (float) $runtime : (float) $options['default_runtime_seconds'];
                if (!is_finite($runtime) || $runtime <= 0.0) {
                    $runtime = (float) $options['default_runtime_seconds'];
                }
                $weight = $depth * $runtime;
                if (!is_finite($weight)) {
                    // Overflow means pressure is at least beyond any useful
                    // scaling threshold. Saturate safely instead of letting
                    // PHP cast ceil(INF) to zero and downscale a live backlog.
                    $nonFinitePressure = true;
                    $weight = PHP_FLOAT_MAX;
                }
                $weights[$queue] = $weight;
            }
        }
        $totalPressure = array_sum($weights);
        $nonFinitePressure = $nonFinitePressure || !is_finite((float) $totalPressure);
        $target = $nonFinitePressure
            ? (int) $options['max_processes']
            : ($totalPressure > 0
            ? (int) ceil($totalPressure / (($options['strategy'] ?? 'size') === 'time'
                ? $options['target_clear_seconds']
                : $options['target_jobs_per_process']))
            : (int) $options['min_processes']);
        $offset = 0;
        if ($totalPressure > 0 && !$nonFinitePressure) {
            $activeQueues = count(array_filter($weights, fn ($weight) => $weight > 0));
            if ($options['balance'] === 'auto') {
                // A positive backlog must not be made permanently unreachable by
                // rounding a small global target onto the first declared queue.
                $target = max($target, $activeQueues);
            }
            // The fleet target is shared, the remainder going to the lowest
            // positions.
            $share = intdiv($target, $replicas) + ($replica < $target % $replicas ? 1 : 0);
            if ($options['balance'] === 'auto') {
                // Replicas start covering the queues with backlog at evenly
                // spaced positions, which move only with the replica or queue
                // count. A share of at least that spacing closes every gap
                // that max_processes leaves room to close.
                $share = max($share, intdiv($activeQueues + $replicas - 1, $replicas));
                $offset = intdiv($replica * $activeQueues, $replicas);
            }
            $target = $share;
        }
        $target = min((int) $options['max_processes'], max((int) $options['min_processes'], $target));

        if ($options['balance'] === 'off') {
            $allocation[$queues[0]] = $target;
            return $allocation;
        }

        $floor = $options['balance'] === 'auto' ? (int) ($options['min_processes_per_queue'] ?? 0) : 0;

        return $this->spread($allocation, max($target, $floor * count($queues)), $weights, $offset, $floor);
    }

    /**
     * $floor workers on every queue, then one worker per queue with backlog
     * still without one, starting at $offset among those queues, then the
     * rest in proportion to the backlog.
     */
    private function spread(array $allocation, int $target, array $weights, int $offset = 0, int $floor = 0): array
    {
        $queues = array_keys($allocation);
        foreach ($queues as $queue) {
            $allocation[$queue] += $floor;
        }
        $target -= $floor * count($queues);
        $active = array_values(array_filter(
            $queues,
            fn ($queue) => ($weights[$queue] ?? 0) > 0 && $allocation[$queue] === 0,
        ));
        $covered = min($target, count($active));
        for ($i = 0; $i < $covered; $i++) {
            $allocation[$active[($offset + $i) % count($active)]]++;
        }
        $target -= $covered;
        for ($i = 0; $i < $target; $i++) {
            $selected = $queues[0];
            $best = -1.0;
            foreach ($queues as $queue) {
                $score = max(1, $weights[$queue] ?? 0) / ($allocation[$queue] + 1);
                if ($score > $best) {
                    $selected = $queue;
                    $best = $score;
                }
            }
            $allocation[$selected]++;
        }

        return $allocation;
    }
}
