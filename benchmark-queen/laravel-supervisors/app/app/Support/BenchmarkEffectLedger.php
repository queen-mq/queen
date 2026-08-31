<?php

namespace App\Support;

use PDO;
use PDOException;
use RuntimeException;
use Throwable;

/**
 * Durable, fixture-local witness for attempts and side effects.
 *
 * An effect is the committed SQLite row itself. This makes crash boundaries
 * observable without pretending that the row is atomic with a queue ACK or
 * with an arbitrary external business side effect.
 */
final class BenchmarkEffectLedger
{
    private const SCHEMA = 'queen.laravel-supervisors.effect-ledger/v1';

    /** @var array<string, PDO> */
    private array $connections = [];

    public function __construct(
        private readonly string $baseDirectory,
        private readonly string $mode,
    ) {
        if (!in_array($this->mode, ['off', 'durable'], true)) {
            throw new RuntimeException('BENCH_LEDGER_MODE must be off or durable.');
        }
        if ($this->baseDirectory === ''
            || !str_starts_with($this->baseDirectory, DIRECTORY_SEPARATOR)
            || rtrim($this->baseDirectory, DIRECTORY_SEPARATOR) === ''
            || preg_match('#(?:^|/)(?:\.|\.\.)(?:/|$)#', $this->baseDirectory) === 1
            || str_contains($this->baseDirectory, "\0")) {
            throw new RuntimeException(
                'BENCH_RESULTS_DIRECTORY must be an absolute, non-root path without dot segments.',
            );
        }
    }

    public function enabled(): bool
    {
        return $this->mode === 'durable';
    }

    public function mode(): string
    {
        return $this->mode;
    }

    /**
     * Create the database before jobs are published. JsonlResultSink must have
     * reserved the run directory already.
     */
    public function reserveRun(string $runId): void
    {
        if (!$this->enabled()) {
            return;
        }

        $this->assertRunId($runId);
        $runDirectory = $this->runDirectory($runId);
        if (!is_dir($runDirectory)) {
            throw new RuntimeException("Benchmark run directory does not exist [{$runDirectory}].");
        }

        $path = $this->databasePath($runId);
        if (file_exists($path)) {
            throw new RuntimeException("Benchmark effect ledger already exists [{$path}].");
        }

        $connection = $this->open($runId);
        $connection->exec(<<<'SQL'
            CREATE TABLE ledger_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE attempts (
                attempt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
                worker_pid INTEGER NOT NULL CHECK (worker_pid > 0),
                worker_host TEXT NOT NULL,
                started_at_ns INTEGER NOT NULL CHECK (started_at_ns >= 0),
                effect_outcome TEXT CHECK (effect_outcome IN ('created', 'already_present')),
                observed_effect_id TEXT,
                effect_observed_at_ns INTEGER CHECK (effect_observed_at_ns >= started_at_ns),
                outcome TEXT CHECK (outcome IN ('completed', 'failed')),
                outcome_at_ns INTEGER CHECK (outcome_at_ns >= started_at_ns),
                error_class TEXT,
                CHECK ((effect_outcome IS NULL AND observed_effect_id IS NULL AND effect_observed_at_ns IS NULL)
                    OR (effect_outcome IS NOT NULL AND observed_effect_id IS NOT NULL
                        AND effect_observed_at_ns IS NOT NULL)),
                CHECK ((outcome IS NULL AND outcome_at_ns IS NULL AND error_class IS NULL)
                    OR (outcome = 'completed' AND outcome_at_ns IS NOT NULL AND error_class IS NULL)
                    OR (outcome = 'failed' AND outcome_at_ns IS NOT NULL AND error_class IS NOT NULL))
            );

            CREATE INDEX attempts_job ON attempts (run_id, job_id, attempt_number);

            CREATE TABLE effects (
                effect_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                created_by_attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
                checksum TEXT NOT NULL,
                committed_at_ns INTEGER NOT NULL CHECK (committed_at_ns >= 0),
                PRIMARY KEY (run_id, job_id)
            ) WITHOUT ROWID;

            CREATE INDEX effects_attempt ON effects (created_by_attempt_id);
            SQL);

        $statement = $connection->prepare('INSERT INTO ledger_meta (key, value) VALUES (?, ?)');
        foreach ([
            ['schema', self::SCHEMA],
            ['run_id', $runId],
            ['semantics', 'fixture-local idempotent effect keyed by run_id+job_id; not queue-ACK atomic'],
        ] as [$key, $value]) {
            $statement->execute([$key, $value]);
        }
    }

    public function startAttempt(
        string $runId,
        string $jobId,
        int $attemptNumber,
        int $startedAtNs,
    ): ?string {
        if (!$this->enabled()) {
            return null;
        }
        $this->assertIdentifiers($runId, $jobId);
        if ($attemptNumber < 1 || $startedAtNs < 0) {
            throw new RuntimeException('Ledger attempt number and timestamp must be positive.');
        }

        $attemptId = bin2hex(random_bytes(16));
        $statement = $this->connection($runId)->prepare(<<<'SQL'
            INSERT INTO attempts (
                attempt_id, run_id, job_id, attempt_number, worker_pid, worker_host, started_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            SQL);
        $statement->execute([
            $attemptId,
            $runId,
            $jobId,
            $attemptNumber,
            getmypid(),
            gethostname() ?: 'unknown',
            $startedAtNs,
        ]);

        return $attemptId;
    }

    /**
     * Atomically create or observe the fixture-local idempotent side effect.
     * The database key is (run_id, job_id); repeated executions remain visible
     * as already_present outcomes on their separate attempt rows.
     *
     * @return array{effect_id: string, created: bool, outcome: string}
     */
    public function commitEffect(
        string $runId,
        string $jobId,
        string $attemptId,
        string $checksum,
        int $committedAtNs,
    ): array {
        if (!$this->enabled()) {
            throw new RuntimeException('Cannot commit an effect while BENCH_LEDGER_MODE is off.');
        }
        $this->assertIdentifiers($runId, $jobId);
        $this->assertToken($attemptId, 'attempt_id');
        if (preg_match('/^[a-f0-9]{64}$/D', $checksum) !== 1 || $committedAtNs < 0) {
            throw new RuntimeException('Ledger effect checksum or timestamp is invalid.');
        }

        $candidateEffectId = bin2hex(random_bytes(16));
        $connection = $this->connection($runId);
        $connection->beginTransaction();
        try {
            $insert = $connection->prepare(<<<'SQL'
                INSERT INTO effects (
                    effect_id, run_id, job_id, created_by_attempt_id, checksum, committed_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, job_id) DO NOTHING
                SQL);
            $insert->execute([
                $candidateEffectId,
                $runId,
                $jobId,
                $attemptId,
                $checksum,
                $committedAtNs,
            ]);
            $created = $insert->rowCount() === 1;

            $select = $connection->prepare(<<<'SQL'
                SELECT effect_id, checksum FROM effects WHERE run_id = ? AND job_id = ?
                SQL);
            $select->execute([$runId, $jobId]);
            $effect = $select->fetch();
            if (!is_array($effect)
                || !is_string($effect['effect_id'] ?? null)
                || ($effect['checksum'] ?? null) !== $checksum) {
                throw new RuntimeException(
                    "Existing ledger effect is missing or has a different checksum [{$runId}:{$jobId}].",
                );
            }

            $outcome = $created ? 'created' : 'already_present';
            $observe = $connection->prepare(<<<'SQL'
                UPDATE attempts
                SET effect_outcome = ?, observed_effect_id = ?, effect_observed_at_ns = ?
                WHERE attempt_id = ? AND run_id = ? AND job_id = ? AND effect_outcome IS NULL
                SQL);
            $observe->execute([
                $outcome,
                $effect['effect_id'],
                $committedAtNs,
                $attemptId,
                $runId,
                $jobId,
            ]);
            if ($observe->rowCount() !== 1) {
                throw new RuntimeException(
                    "Ledger attempt is missing or already observed an effect [{$attemptId}].",
                );
            }
            $connection->commit();

            return [
                'effect_id' => $effect['effect_id'],
                'created' => $created,
                'outcome' => $outcome,
            ];
        } catch (Throwable $exception) {
            if ($connection->inTransaction()) {
                $connection->rollBack();
            }
            throw $exception;
        }
    }

    public function completeAttempt(string $runId, string $attemptId, int $completedAtNs): void
    {
        $this->finishAttempt($runId, $attemptId, 'completed', $completedAtNs, null);
    }

    public function failAttempt(
        string $runId,
        string $attemptId,
        int $failedAtNs,
        Throwable $exception,
    ): void {
        $this->finishAttempt(
            $runId,
            $attemptId,
            'failed',
            $failedAtNs,
            $exception::class,
        );
    }

    /** @return array{mode: string, busy: int, wal_frames: int, checkpointed_frames: int} */
    public function checkpointRun(string $runId): array
    {
        if (!$this->enabled()) {
            return ['mode' => 'off', 'busy' => 0, 'wal_frames' => 0, 'checkpointed_frames' => 0];
        }

        $row = $this->connection($runId)
            ->query('PRAGMA wal_checkpoint(TRUNCATE)')
            ->fetch(PDO::FETCH_NUM);
        if (!is_array($row) || count($row) !== 3) {
            throw new RuntimeException('SQLite did not return a valid WAL checkpoint result.');
        }
        [$busy, $walFrames, $checkpointedFrames] = array_map('intval', $row);
        if ($busy !== 0) {
            throw new RuntimeException("SQLite WAL checkpoint remained busy for run [{$runId}].");
        }

        return [
            'mode' => 'durable',
            'busy' => $busy,
            'wal_frames' => $walFrames,
            'checkpointed_frames' => $checkpointedFrames,
        ];
    }

    private function finishAttempt(
        string $runId,
        string $attemptId,
        string $outcome,
        int $outcomeAtNs,
        ?string $errorClass,
    ): void {
        if (!$this->enabled()) {
            return;
        }
        $this->assertRunId($runId);
        $this->assertToken($attemptId, 'attempt_id');
        if ($outcomeAtNs < 0) {
            throw new RuntimeException('Ledger outcome timestamp must be non-negative.');
        }

        $statement = $this->connection($runId)->prepare(<<<'SQL'
            UPDATE attempts
            SET outcome = ?, outcome_at_ns = ?, error_class = ?
            WHERE attempt_id = ? AND run_id = ? AND outcome IS NULL
            SQL);
        $statement->execute([$outcome, $outcomeAtNs, $errorClass, $attemptId, $runId]);
        if ($statement->rowCount() !== 1) {
            throw new RuntimeException("Ledger attempt is missing or already finalized [{$attemptId}].");
        }
    }

    private function connection(string $runId): PDO
    {
        $this->assertRunId($runId);
        if (!isset($this->connections[$runId])) {
            $path = $this->databasePath($runId);
            if (!is_file($path)) {
                throw new RuntimeException("Benchmark effect ledger does not exist [{$path}].");
            }
            $this->connections[$runId] = $this->open($runId);
        }

        return $this->connections[$runId];
    }

    private function open(string $runId): PDO
    {
        $path = $this->databasePath($runId);
        try {
            $connection = new PDO('sqlite:'.$path, options: [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::ATTR_TIMEOUT => 60,
            ]);
            $connection->exec('PRAGMA busy_timeout = 60000');
            $connection->exec('PRAGMA foreign_keys = ON');
            $connection->exec('PRAGMA journal_mode = WAL');
            $connection->exec('PRAGMA synchronous = FULL');
            $connection->exec('PRAGMA wal_autocheckpoint = 1000');
        } catch (PDOException $exception) {
            throw new RuntimeException("Unable to open benchmark effect ledger [{$path}].", previous: $exception);
        }

        $this->connections[$runId] = $connection;

        return $connection;
    }

    private function databasePath(string $runId): string
    {
        return $this->runDirectory($runId).DIRECTORY_SEPARATOR.'ledger.sqlite3';
    }

    private function runDirectory(string $runId): string
    {
        return rtrim($this->baseDirectory, DIRECTORY_SEPARATOR).DIRECTORY_SEPARATOR.$runId;
    }

    private function assertIdentifiers(string $runId, string $jobId): void
    {
        $this->assertRunId($runId);
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $jobId) !== 1) {
            throw new RuntimeException('Benchmark ledger job_id has an invalid format.');
        }
    }

    private function assertRunId(string $runId): void
    {
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new RuntimeException('Benchmark ledger run_id has an invalid format.');
        }
    }

    private function assertToken(string $value, string $label): void
    {
        if (preg_match('/^[a-f0-9]{32}$/D', $value) !== 1) {
            throw new RuntimeException("Benchmark ledger {$label} has an invalid format.");
        }
    }
}
