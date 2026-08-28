<?php

namespace Queen\Laravel\Supervisor;

final class TelemetryReader
{
    /** @return array<string, float> */
    public function runtimes(string $directory, int $ttlSeconds, ?array $scope = null): array
    {
        $totals = [];
        $samples = [];
        foreach (glob(rtrim($directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . '*.json') ?: [] as $file) {
            if (time() - filemtime($file) > $ttlSeconds) {
                @unlink($file);
                continue;
            }
            $document = json_decode((string) file_get_contents($file), true);
            if ($scope !== null && (
                ($document['supervisor'] ?? null) !== $scope['supervisor']
                || ($document['connection'] ?? null) !== $scope['connection']
                || ($document['consumer_group'] ?? null) !== $scope['consumer_group']
            )) {
                continue;
            }
            foreach (($document['queues'] ?? []) as $queue => $stats) {
                $count = min(100, max(0, (int) ($stats['samples'] ?? 0)));
                $totals[$queue] = ($totals[$queue] ?? 0.0) + ((float) ($stats['runtime_ewma_seconds'] ?? 0.0) * $count);
                $samples[$queue] = ($samples[$queue] ?? 0) + $count;
            }
        }

        $result = [];
        foreach ($totals as $queue => $total) {
            if (($samples[$queue] ?? 0) > 0) {
                $result[$queue] = $total / $samples[$queue];
            }
        }
        return $result;
    }
}
