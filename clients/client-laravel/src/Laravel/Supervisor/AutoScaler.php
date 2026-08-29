<?php

namespace Queen\Laravel\Supervisor;

final class AutoScaler
{
    /** @return array<string, int> */
    public function desired(array $options, array $depths, array $runtimes = []): array
    {
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
        if ($totalPressure > 0 && $options['balance'] === 'auto') {
            // A positive backlog must not be made permanently unreachable by
            // rounding a small global target onto the first declared queue.
            $activeQueues = count(array_filter($weights, fn ($weight) => $weight > 0));
            $target = max($target, $activeQueues);
        }
        $target = min((int) $options['max_processes'], max((int) $options['min_processes'], $target));

        if ($options['balance'] === 'off') {
            $allocation[$queues[0]] = $target;
            return $allocation;
        }

        return $this->spread($allocation, $target, $weights);
    }

    private function spread(array $allocation, int $target, array $weights): array
    {
        $queues = array_keys($allocation);
        foreach ($queues as $queue) {
            if ($target > 0 && ($weights[$queue] ?? 0) > 0) {
                $allocation[$queue]++;
                $target--;
            }
        }
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
