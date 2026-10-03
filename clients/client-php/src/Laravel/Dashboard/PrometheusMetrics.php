<?php

namespace Queen\Laravel\Dashboard;

/**
 * The supervision snapshot in the Prometheus text exposition format (0.0.4),
 * for scrapers and the autoscalers built on them (Kubernetes HPA through
 * prometheus-adapter, KEDA's Prometheus scaler).
 *
 * Every value comes from DashboardRepository::supervision(), which already
 * normalized the untrusted status documents; labels are escaped here.
 */
final class PrometheusMetrics
{
    /** @param array<string, mixed> $supervision */
    public static function render(array $supervision): string
    {
        $metrics = new self();

        $metrics->family('queen_queue_depth', 'Jobs waiting per queue and consumer group, as last sampled by a supervisor.');
        foreach ($supervision['queues'] as $queue) {
            if ($queue['available']) {
                $metrics->sample('queen_queue_depth', self::queueLabels($queue), $queue['depth']);
            }
        }

        $metrics->family('queen_supervisor_instances', 'Supervisor instances by availability.');
        $availability = ['live' => 0, 'stale' => 0];
        foreach ($supervision['instances'] as $instance) {
            $availability[$instance['availability']] = ($availability[$instance['availability']] ?? 0) + 1;
        }
        foreach ($availability as $state => $count) {
            $metrics->sample('queen_supervisor_instances', ['availability' => $state], $count);
        }

        $metrics->family('queen_supervisor_up', 'One when the supervisor instance has a current heartbeat.');
        $metrics->family('queen_supervisor_heartbeat_age_seconds', 'Seconds since the supervisor instance last wrote its status.');
        $metrics->family('queen_workers', 'Worker processes running per pool.');
        $metrics->family('queen_workers_desired', 'Worker processes the pool is scaling to.');
        $metrics->family('queen_workers_draining', 'Worker processes finishing their job before they exit.');
        $metrics->family('queen_pool_replicas', 'Coordinated replicas sharing the pool target.');
        foreach ($supervision['instances'] as $instance) {
            $labels = ['instance_id' => $instance['instance_id'], 'hostname' => $instance['hostname'] ?? ''];
            $metrics->sample('queen_supervisor_up', $labels, $instance['availability'] === 'live' ? 1 : 0);
            $metrics->sample('queen_supervisor_heartbeat_age_seconds', $labels, $instance['age_seconds']);
            foreach ($instance['pools'] as $pool) {
                $poolLabels = [...$labels, 'supervisor' => $pool['supervisor'], 'queue' => $pool['queue']];
                $metrics->sample('queen_workers', $poolLabels, $pool['processes']);
                $metrics->sample('queen_workers_desired', $poolLabels, $pool['desired']);
                $metrics->sample('queen_workers_draining', $poolLabels, $pool['draining']);
                if ($pool['replicas'] !== null) {
                    $metrics->sample('queen_pool_replicas', $poolLabels, $pool['replicas']);
                }
            }
        }

        $metrics->family(
            'queen_shared_queue_supervisors',
            'Running supervisors that autoscale the same queue without sharing one target.',
        );
        foreach ($supervision['shared_queues'] as $shared) {
            $metrics->sample('queen_shared_queue_supervisors', self::queueLabels($shared), $shared['instances']);
        }

        return implode('', array_map(fn (array $lines): string => implode('', $lines), $metrics->families));
    }

    /** @var array<string, list<string>> family name => its HELP, TYPE and samples, kept together */
    private array $families = [];

    private function family(string $name, string $help): void
    {
        $this->families[$name] = ["# HELP {$name} {$help}\n", "# TYPE {$name} gauge\n"];
    }

    /** @param array<string, string> $labels */
    private function sample(string $name, array $labels, int $value): void
    {
        $pairs = [];
        foreach ($labels as $label => $labelValue) {
            $pairs[] = $label . '="' . self::escape($labelValue) . '"';
        }
        $this->families[$name][] = $name . ($pairs === [] ? '' : '{' . implode(',', $pairs) . '}') . ' ' . $value . "\n";
    }

    /** @param array<string, mixed> $queue */
    private static function queueLabels(array $queue): array
    {
        return [
            'connection' => $queue['connection'],
            'consumer_group' => $queue['consumer_group'],
            'queue' => $queue['queue'],
        ];
    }

    private static function escape(string $value): string
    {
        return str_replace(['\\', '"', "\n"], ['\\\\', '\\"', '\\n'], $value);
    }
}
