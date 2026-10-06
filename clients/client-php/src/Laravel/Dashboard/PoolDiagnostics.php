<?php

namespace Queen\Laravel\Dashboard;

/** Interprets one heartbeat. It does not infer throughput or a root cause. */
final class PoolDiagnostics
{
    /** @return list<array<string, mixed>> */
    public static function forInstance(array $instance): array
    {
        $rows = [];
        foreach ($instance['pools'] as $pool) {
            $finding = self::finding($instance, $pool);
            $rows[] = array_merge($pool, $finding);
        }
        usort($rows, static fn (array $a, array $b): int =>
            ($b['priority'] <=> $a['priority'])
            ?: (($b['depth_available'] ? $b['depth'] : -1) <=> ($a['depth_available'] ? $a['depth'] : -1))
            ?: ([$a['supervisor'], $a['queue']] <=> [$b['supervisor'], $b['queue']]));

        return $rows;
    }

    private static function finding(array $instance, array $pool): array
    {
        $result = static fn (string $label, string $next, string $tone = '', int $priority = 0): array =>
            compact('label', 'next', 'tone', 'priority');

        if ($instance['availability'] !== 'live') {
            return $result('Current state unknown', 'Check the master process and heartbeat. These are last reported values.', 'warning', 1);
        }
        if ($instance['state'] !== 'running') {
            return match ($instance['state']) {
                'paused' => $result('Supervisor paused', 'Continue the supervisor when it should resume consuming.'),
                'terminating' => $result('Supervisor terminating', 'Let in-flight jobs finish; check worker logs if shutdown stalls.'),
                'starting' => $result('Supervisor starting', 'Check the next heartbeat for worker readiness.'),
                default => $result('Supervisor not running', 'Check the master process before diagnosing pool capacity.', 'warning', 1),
            };
        }
        if (in_array($pool['restart_state'], ['open', 'backoff', 'probe'], true)) {
            return $result(
                match ($pool['restart_state']) { 'open' => 'Restart circuit open', 'backoff' => 'Restart backoff', default => 'Restart probe' },
                'Check worker exit logs and the restart failure count before adding capacity.',
                $pool['restart_state'] === 'open' ? 'danger' : 'warning',
                $pool['restart_state'] === 'open' ? 5 : 4,
            );
        }
        $countsKnown = $pool['counts_available'] ?? false;
        if ($countsKnown && $pool['processes'] === 0 && $pool['depth_available'] && $pool['depth'] > 0) {
            return $result('Pending work, no workers', $pool['desired'] === 0
                ? 'The target is zero. Check this pool’s balancing settings and configured limits.'
                : 'Workers are requested but none are running. Check startup and worker exit logs.', 'danger', 5);
        }
        if ($countsKnown && $pool['processes'] < $pool['desired']) {
            $budget = $instance['process_budget'];
            $cost = $pool['process_cost_per_worker'];
            if ($budget['valid'] && $cost !== null && $budget['available'] < $cost) {
                return $result('No process headroom', 'The shared budget cannot fit another worker. Review draining processes and the process limit.', 'warning', 3);
            }
            return $result('Below desired capacity', 'Check the next heartbeat for convergence, then worker startup logs if the gap persists.', 'warning', 3);
        }
        if (!$countsKnown || !$pool['depth_available']) {
            return $result('Incomplete telemetry', !$pool['depth_available']
                ? 'Queue depth is unavailable. Check broker connectivity before judging the backlog.'
                : 'Worker counts are not reported. Check the supervisor status publication.', 'warning', 2);
        }
        if (!$pool['ready'] || !$pool['healthy']) {
            return $result('Readiness not confirmed', 'Check worker health and the next heartbeat; matching counts alone do not confirm readiness.', 'warning', 2);
        }
        if ($pool['draining'] > 0) {
            return $result('Workers draining', 'Allow in-flight jobs to finish. Inspect job duration and timeouts if draining persists.', '', 1);
        }

        return $result($pool['desired'] === 0 ? 'Idle · zero target' : 'At desired capacity',
            $pool['depth'] > 0 ? 'Use Workload to check whether the backlog is growing.' : 'No capacity issue observed in this heartbeat.');
    }
}
