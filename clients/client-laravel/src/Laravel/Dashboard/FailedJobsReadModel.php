<?php

namespace Queen\Laravel\Dashboard;

use Illuminate\Contracts\Config\Repository as ConfigRepository;
use RuntimeException;

/**
 * A bounded, read-only projection of Laravel's failed store.
 *
 * It intentionally does not call FailedJobProviderInterface::find() (Queen's
 * synchronized provider attaches a retry fence there) or all() (unbounded).
 */
final class FailedJobsReadModel
{
    private const MAX_FILE_BYTES = 4194304;

    /** @param \Closure(?string): mixed $databaseConnection */
    public function __construct(
        private ConfigRepository $config,
        private \Closure $databaseConnection,
    ) {
    }

    /** @return array{total: int, total_exact: bool, records: list<mixed>} */
    public function read(int $limit): array
    {
        $failed = $this->config->get('queue.failed', []);
        if (!is_array($failed)) {
            throw new RuntimeException('Failed-job storage is unavailable.');
        }
        $driver = $failed['driver'] ?? null;

        return match ($driver) {
            'database', 'database-uuids' => $this->database($failed, $driver, $limit),
            'file' => $this->file($failed, $limit),
            'null' => ['total' => 0, 'total_exact' => true, 'records' => []],
            default => throw new RuntimeException('Failed-job storage is not supported by the dashboard.'),
        };
    }

    /**
     * @param array<string, mixed> $failed
     * @return array{total: int, total_exact: bool, records: list<mixed>}
     */
    private function database(array $failed, string $driver, int $limit): array
    {
        $table = $failed['table'] ?? null;
        $database = $failed['database'] ?? null;
        if (!is_string($table) || $table === '' || (!is_string($database) && $database !== null)) {
            throw new RuntimeException('Failed-job database configuration is unavailable.');
        }
        $connection = ($this->databaseConnection)($database);
        if (!is_object($connection) || !method_exists($connection, 'table')) {
            throw new RuntimeException('Failed-job database connection is unavailable.');
        }

        $identifier = $driver === 'database-uuids' ? 'uuid' : 'id';
        $query = $connection->table($table);
        $records = $query
            ->select([$identifier, 'connection', 'queue', 'failed_at'])
            ->orderBy('id', 'desc')
            // Fetch one sentinel row instead of COUNT(*). The dashboard needs
            // a bounded operational summary, not an exact full-table scan on
            // every refresh.
            ->limit($limit + 1)
            ->get()
            ->map(function (mixed $record) use ($identifier): array {
                $record = is_object($record) ? (array) $record : (is_array($record) ? $record : []);

                return [
                    'id' => $record[$identifier] ?? null,
                    'connection' => $record['connection'] ?? null,
                    'queue' => $record['queue'] ?? null,
                    'failed_at' => $record['failed_at'] ?? null,
                ];
            })
            ->all();

        if (!is_array($records)) {
            throw new RuntimeException('Failed-job database returned malformed metadata.');
        }
        $hasMore = count($records) > $limit;
        $records = array_slice($records, 0, $limit);

        return [
            'total' => $hasMore ? $limit + 1 : count($records),
            'total_exact' => !$hasMore,
            'records' => array_values($records),
        ];
    }

    /**
     * @param array<string, mixed> $failed
     * @return array{total: int, total_exact: bool, records: list<mixed>}
     */
    private function file(array $failed, int $limit): array
    {
        $path = $failed['path'] ?? null;
        $configuredLimit = $failed['limit'] ?? 100;
        if (is_string($configuredLimit) && preg_match('/^[0-9]+$/D', $configuredLimit) === 1) {
            $configuredLimit = filter_var($configuredLimit, FILTER_VALIDATE_INT);
        }
        if (!is_string($path) || $path === '' || !is_int($configuredLimit) || $configuredLimit < 1 || $configuredLimit > 10000) {
            throw new RuntimeException('Failed-job file configuration is unavailable.');
        }
        $metadata = @lstat($path);
        if ($metadata === false) {
            return ['total' => 0, 'total_exact' => true, 'records' => []];
        }
        if (($metadata['mode'] & 0170000) !== 0100000 || $metadata['size'] < 0 || $metadata['size'] > self::MAX_FILE_BYTES) {
            throw new RuntimeException('Failed-job file is not a bounded regular file.');
        }

        $handle = @fopen($path, 'rb');
        if ($handle === false) {
            throw new RuntimeException('Failed-job file is unavailable.');
        }
        try {
            $current = @lstat($path);
            $opened = fstat($handle);
            if (!is_array($current)
                || !is_array($opened)
                || ($current['mode'] & 0170000) !== 0100000
                || ($opened['mode'] & 0170000) !== 0100000
                || $current['dev'] !== $opened['dev']
                || $current['ino'] !== $opened['ino']
                || $opened['size'] < 0
                || $opened['size'] > self::MAX_FILE_BYTES) {
                throw new RuntimeException('Failed-job file changed while opening it.');
            }
            $contents = stream_get_contents($handle, self::MAX_FILE_BYTES + 1);
        } finally {
            fclose($handle);
        }
        if (!is_string($contents) || strlen($contents) > self::MAX_FILE_BYTES) {
            throw new RuntimeException('Failed-job file exceeds the dashboard read bound.');
        }
        if (trim($contents) === '') {
            return ['total' => 0, 'total_exact' => true, 'records' => []];
        }
        $records = json_decode($contents, true, 32);
        if (!is_array($records) || !array_is_list($records) || count($records) > $configuredLimit) {
            throw new RuntimeException('Failed-job file contains malformed or unbounded metadata.');
        }

        return [
            'total' => count($records),
            'total_exact' => true,
            'records' => array_slice($records, 0, $limit),
        ];
    }
}
