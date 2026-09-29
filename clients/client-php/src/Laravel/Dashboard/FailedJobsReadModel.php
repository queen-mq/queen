<?php

namespace Queen\Laravel\Dashboard;

use Illuminate\Contracts\Config\Repository as ConfigRepository;
use RuntimeException;

/**
 * A bounded, read-only projection of Laravel's failed store.
 *
 * It intentionally does not call FailedJobProviderInterface::find() (Queen's
 * synchronized provider attaches a retry fence there) or all() (unbounded).
 *
 * Pages use a keyset cursor: the database id of the last row shown (newest
 * first), or the position of the last record in a failed-job file. A page is
 * therefore one indexed `id < ?` range with a sentinel row, never an OFFSET
 * scan or a COUNT(*) over a table that may hold millions of rows.
 */
final class FailedJobsReadModel
{
    private const MAX_FILE_BYTES = 4194304;

    /** Bytes of exception text and payload read for one detail record. */
    public const MAX_DETAIL_BYTES = 1048576;

    /** @param \Closure(?string): mixed $databaseConnection */
    public function __construct(
        private ConfigRepository $config,
        private \Closure $databaseConnection,
    ) {
    }

    /**
     * @return array{total: int, total_exact: bool, records: list<mixed>, next_cursor: ?int}
     */
    public function read(int $limit, ?int $cursor = null): array
    {
        [$failed, $driver] = $this->store();

        return match ($driver) {
            'database', 'database-uuids' => $this->database($failed, $driver, $limit, $cursor),
            'file' => $this->file($failed, $limit, $cursor),
            'null' => ['total' => 0, 'total_exact' => true, 'records' => [], 'next_cursor' => null],
            default => throw new RuntimeException('Failed-job storage is not supported by the dashboard.'),
        };
    }

    /**
     * One failed job by its Laravel identifier (the uuid for database-uuids,
     * otherwise the id), with its exception and payload bounded in size.
     *
     * @return array<string, mixed>|null
     */
    public function find(string $id): ?array
    {
        [$failed, $driver] = $this->store();

        return match ($driver) {
            'database', 'database-uuids' => $this->findInDatabase($failed, $driver, $id),
            'file' => $this->findInFile($failed, $id),
            'null' => null,
            default => throw new RuntimeException('Failed-job storage is not supported by the dashboard.'),
        };
    }

    /** @return array{0: array<string, mixed>, 1: mixed} */
    private function store(): array
    {
        $failed = $this->config->get('queue.failed', []);
        if (!is_array($failed)) {
            throw new RuntimeException('Failed-job storage is unavailable.');
        }

        return [$failed, $failed['driver'] ?? null];
    }

    /**
     * @param array<string, mixed> $failed
     * @return array<string, mixed>|null
     */
    private function findInDatabase(array $failed, string $driver, string $id): ?array
    {
        $identifier = $driver === 'database-uuids' ? 'uuid' : 'id';
        if ($identifier === 'id' && preg_match('/^[0-9]{1,18}$/D', $id) !== 1) {
            return null;
        }
        $query = $this->table($failed)->select([$identifier, 'connection', 'queue', 'failed_at']);
        // Cut the two text columns in SQL: a failed row can hold a payload of
        // many megabytes, and loading it whole before truncating would make
        // one page view cost that much PHP memory.
        $substring = $query->getConnection()->getDriverName() === 'sqlsrv' ? 'SUBSTRING' : 'SUBSTR';
        foreach (['exception', 'payload'] as $column) {
            $wrapped = $query->getGrammar()->wrap($column);
            $query->selectRaw("{$substring}({$wrapped}, 1, " . self::MAX_DETAIL_BYTES . ") as {$wrapped}");
        }
        $record = $query
            ->where($identifier, $identifier === 'id' ? (int) $id : $id)
            ->limit(1)
            ->first();
        if ($record === null) {
            return null;
        }
        $record = is_object($record) ? (array) $record : (is_array($record) ? $record : []);

        return [
            'id' => $record[$identifier] ?? null,
            'connection' => $record['connection'] ?? null,
            'queue' => $record['queue'] ?? null,
            'failed_at' => $record['failed_at'] ?? null,
            'exception' => $this->bounded($record['exception'] ?? null),
            'payload' => $this->bounded($record['payload'] ?? null),
        ];
    }

    /**
     * @param array<string, mixed> $failed
     * @return array<string, mixed>|null
     */
    private function findInFile(array $failed, string $id): ?array
    {
        foreach ($this->fileRecords($failed) as $record) {
            if (is_array($record) && isset($record['id']) && (string) $record['id'] === $id) {
                return [
                    'id' => $record['id'],
                    'connection' => $record['connection'] ?? null,
                    'queue' => $record['queue'] ?? null,
                    'failed_at' => $record['failed_at'] ?? null,
                    'exception' => $this->bounded($record['exception'] ?? null),
                    'payload' => $this->bounded($record['payload'] ?? null),
                ];
            }
        }

        return null;
    }

    private function bounded(mixed $value): ?string
    {
        return is_string($value) ? substr($value, 0, self::MAX_DETAIL_BYTES) : null;
    }

    /** @param array<string, mixed> $failed */
    private function table(array $failed): mixed
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

        return $connection->table($table);
    }

    /**
     * @param array<string, mixed> $failed
     * @return array{total: int, total_exact: bool, records: list<mixed>, next_cursor: ?int}
     */
    private function database(array $failed, string $driver, int $limit, ?int $cursor): array
    {
        $identifier = $driver === 'database-uuids' ? 'uuid' : 'id';
        $query = $this->table($failed);
        if ($cursor !== null) {
            $query->where('id', '<', $cursor);
        }
        $records = $query
            ->select(array_values(array_unique(['id', $identifier, 'connection', 'queue', 'failed_at'])))
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
                    'cursor' => $record['id'] ?? null,
                ];
            })
            ->all();

        if (!is_array($records)) {
            throw new RuntimeException('Failed-job database returned malformed metadata.');
        }
        $hasMore = count($records) > $limit;
        $records = array_values(array_slice($records, 0, $limit));
        $last = $records === [] ? null : $records[count($records) - 1]['cursor'];

        return [
            'total' => $hasMore ? $limit + 1 : count($records),
            'total_exact' => !$hasMore,
            'records' => $records,
            'next_cursor' => $hasMore && is_numeric($last) ? (int) $last : null,
        ];
    }

    /**
     * @param array<string, mixed> $failed
     * @return array{total: int, total_exact: bool, records: list<mixed>, next_cursor: ?int}
     */
    private function file(array $failed, int $limit, ?int $cursor): array
    {
        $records = $this->fileRecords($failed);
        // The cursor is the 1-based position of the last record shown.
        $offset = $cursor ?? 0;
        $page = array_slice($records, $offset, $limit);
        $next = $offset + count($page);

        return [
            'total' => count($records),
            'total_exact' => true,
            'records' => $page,
            'next_cursor' => $next < count($records) ? $next : null,
        ];
    }

    /**
     * @param array<string, mixed> $failed
     * @return list<mixed>
     */
    private function fileRecords(array $failed): array
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
            return [];
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
            return [];
        }
        $records = json_decode($contents, true, 32);
        if (!is_array($records) || !array_is_list($records) || count($records) > $configuredLimit) {
            throw new RuntimeException('Failed-job file contains malformed or unbounded metadata.');
        }

        return $records;
    }
}
