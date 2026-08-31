<?php

namespace Queen\Laravel\Supervisor;

final class TelemetryReader
{
    private const MAX_FILE_BYTES = 65536;

    private const MAX_FILES = 4096;

    private const MAX_DIRECTORY_ENTRIES = 8192;

    private const MAX_TOTAL_BYTES = 16777216;

    private const MAX_QUEUES_PER_FILE = 256;

    /** @return array<string, float> */
    public function runtimes(string $directory, int $ttlSeconds, ?array $scope = null): array
    {
        $directory = rtrim($directory, DIRECTORY_SEPARATOR);
        $directoryMetadata = @lstat($directory);
        if ($directoryMetadata === false) {
            return [];
        }
        if (($directoryMetadata['mode'] & 0170000) !== 0040000
            || ($directoryMetadata['mode'] & 07777) !== 0700
            || (function_exists('posix_geteuid') && ($directoryMetadata['uid'] ?? null) !== posix_geteuid())) {
            return [];
        }

        try {
            $iterator = new \FilesystemIterator($directory, \FilesystemIterator::SKIP_DOTS);
        } catch (\Throwable) {
            return [];
        }
        /** @var list<array{path:string,mtime:int,size:int,dev:int,ino:int,mode:int}> $files */
        $files = [];
        $entries = 0;
        $overflow = false;
        $now = time();
        foreach ($iterator as $entry) {
            if ($entries >= self::MAX_DIRECTORY_ENTRIES) {
                $overflow = true;
                break;
            }
            $entries++;
            if (!$entry instanceof \SplFileInfo) {
                continue;
            }
            $filename = $entry->getFilename();
            $isDocument = str_ends_with($filename, '.json');
            $isTemporary = preg_match('/\.json\.[a-f0-9]{16}\.tmp$/D', $filename) === 1;
            if (!$isDocument && !$isTemporary) {
                continue;
            }
            $path = $entry->getPathname();
            $metadata = @lstat($path);
            if (!is_array($metadata)) {
                continue;
            }
            $mtime = is_int($metadata['mtime'] ?? null) ? $metadata['mtime'] : $now;
            if ($now - $mtime > $ttlSeconds) {
                $this->removeIfSameFile($path, $metadata);
                continue;
            }
            if (!$isDocument
                || ($metadata['mode'] & 0170000) !== 0100000
                || ($metadata['mode'] & 07777) !== 0600
                || ($metadata['uid'] ?? null) !== ($directoryMetadata['uid'] ?? null)
                || !is_int($metadata['size'] ?? null)
                || $metadata['size'] < 1
                || $metadata['size'] > self::MAX_FILE_BYTES) {
                continue;
            }
            $files[] = [
                'path' => $path,
                'mtime' => $mtime,
                'size' => $metadata['size'],
                'dev' => $metadata['dev'],
                'ino' => $metadata['ino'],
                'mode' => $metadata['mode'],
            ];
        }

        // Keep a bounded newest reservoir. Short-lived workers (for example
        // --max-jobs=1) may legitimately produce more PID files than the
        // retention window between two polls. Pruning the oldest files both
        // preserves recent samples and makes an over-cap directory recover on
        // subsequent bounded scans instead of poisoning telemetry forever.
        usort($files, static fn (array $left, array $right): int => [$right['mtime'], $right['path']]
            <=> [$left['mtime'], $left['path']]);
        $selected = [];
        $selectedBytes = 0;
        foreach ($files as $file) {
            if (count($selected) < self::MAX_FILES
                && $selectedBytes <= self::MAX_TOTAL_BYTES - $file['size']) {
                $selected[] = $file;
                $selectedBytes += $file['size'];
                continue;
            }
            $this->removeIfSameFile($file['path'], $file);
        }
        if ($overflow) {
            return [];
        }

        $totals = [];
        $samples = [];
        $totalBytes = 0;
        foreach ($selected as $candidate) {
            $file = $candidate['path'];
            $metadata = @lstat($file);
            if ($metadata === false
                || ($metadata['mode'] & 0170000) !== 0100000
                || ($metadata['mode'] & 07777) !== 0600
                || ($metadata['uid'] ?? null) !== ($directoryMetadata['uid'] ?? null)
                || $metadata['size'] < 1
                || $metadata['size'] > self::MAX_FILE_BYTES) {
                continue;
            }
            if ($now - ($metadata['mtime'] ?? $now) > $ttlSeconds) {
                $this->removeIfSameFile($file, $metadata);
                continue;
            }
            $handle = @fopen($file, 'rb');
            if ($handle === false) {
                continue;
            }
            $current = @lstat($file);
            $opened = fstat($handle);
            if (!is_array($current)
                || !is_array($opened)
                || ($current['mode'] & 0170000) !== 0100000
                || ($opened['mode'] & 0170000) !== 0100000
                || ($opened['mode'] & 07777) !== 0600
                || ($opened['uid'] ?? null) !== ($directoryMetadata['uid'] ?? null)
                || $current['dev'] !== $opened['dev']
                || $current['ino'] !== $opened['ino']
                || $opened['size'] < 1
                || $opened['size'] > self::MAX_FILE_BYTES) {
                fclose($handle);
                continue;
            }
            $contents = stream_get_contents($handle, self::MAX_FILE_BYTES + 1);
            fclose($handle);
            if (!is_string($contents) || $contents === '' || strlen($contents) > self::MAX_FILE_BYTES) {
                continue;
            }
            // Count bytes from the opened inode rather than the pre-open
            // directory entry. A concurrent replacement can therefore never
            // evade the aggregate parsing budget.
            $totalBytes += strlen($contents);
            if ($totalBytes > self::MAX_TOTAL_BYTES) {
                return [];
            }
            try {
                $document = json_decode($contents, true, 16, JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                continue;
            }
            if (!is_array($document) || !is_array($document['queues'] ?? null)
                || count($document['queues']) > self::MAX_QUEUES_PER_FILE) {
                continue;
            }
            if ($scope !== null && (
                ($document['supervisor'] ?? null) !== $scope['supervisor']
                || ($document['connection'] ?? null) !== $scope['connection']
                || ($document['consumer_group'] ?? null) !== $scope['consumer_group']
            )) {
                continue;
            }
            foreach (($document['queues'] ?? []) as $queue => $stats) {
                if (!is_string($queue)
                    || $queue === ''
                    || strlen($queue) > 256
                    || preg_match('/[\x00-\x1F\x7F]/', $queue) === 1
                    || !is_array($stats)) {
                    continue;
                }
                $count = min(100, max(0, (int) ($stats['samples'] ?? 0)));
                $runtime = (float) ($stats['runtime_ewma_seconds'] ?? 0.0);
                if ($count === 0 || !is_finite($runtime) || $runtime <= 0.0) {
                    continue;
                }
                $weighted = $runtime * $count;
                $nextTotal = ($totals[$queue] ?? 0.0) + $weighted;
                if (!is_finite($weighted) || !is_finite($nextTotal)) {
                    continue;
                }
                $totals[$queue] = $nextTotal;
                $samples[$queue] = ($samples[$queue] ?? 0) + $count;
            }
        }

        $result = [];
        foreach ($totals as $queue => $total) {
            if (($samples[$queue] ?? 0) > 0) {
                $average = $total / $samples[$queue];
                if (is_finite($average) && $average > 0.0) {
                    $result[$queue] = $average;
                }
            }
        }
        return $result;
    }

    /** @param array<string, int> $opened */
    private function removeIfSameFile(string $path, array $opened): void
    {
        $current = @lstat($path);
        if (!is_array($current)
            || ($current['mode'] & 0170000) !== 0100000
            || (($opened['mode'] ?? 0) & 0170000) !== 0100000
            || ($current['dev'] ?? null) !== ($opened['dev'] ?? null)
            || ($current['ino'] ?? null) !== ($opened['ino'] ?? null)) {
            return;
        }

        @unlink($path);
    }
}
