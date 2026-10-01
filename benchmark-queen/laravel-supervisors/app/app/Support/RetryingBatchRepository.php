<?php

namespace App\Support;

use Closure;
use Illuminate\Bus\DatabaseBatchRepository;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Log;

/**
 * Laravel's batch repository, made safe for the compatibility lanes' SQLite.
 *
 * Laravel updates a batch row in a deferred transaction that reads, then
 * writes. In WAL mode, SQLite refuses that upgrade at once with "database is
 * locked" while another worker holds the write lock, whatever busy_timeout
 * is; an immediate retry is refused the same way. BEGIN IMMEDIATE avoids it,
 * but Laravel asks for it (`transaction_mode`) on PHP 8.4 only, and the image
 * runs PHP 8.3. So each transaction writes first: a no-op update takes the
 * write lock, waiting for it under busy_timeout, before the row is read.
 * Laravel retries "database is locked" only when a transaction has more than
 * one attempt: five remain, for anything else that SQLite refuses.
 */
final class RetryingBatchRepository extends DatabaseBatchRepository
{
    private const ATTEMPTS = 5;

    protected function updateAtomicValues(string $batchId, Closure $callback)
    {
        return $this->connection->transaction(function () use ($batchId, $callback) {
            try {
                $this->connection->table($this->table)->where('id', $batchId)->update(['id' => $batchId]);
                $batch = $this->connection->table($this->table)->where('id', $batchId)
                    ->lockForUpdate()
                    ->first();

                return is_null($batch) ? [] : tap($callback($batch), function ($values) use ($batchId) {
                    $this->connection->table($this->table)->where('id', $batchId)->update($values);
                });
            } catch (QueryException $error) {
                // In the lane's log: an attempt failed, and whether the next one ran.
                Log::warning('A batch update failed; the transaction runs again while attempts remain.',
                    ['batch' => $batchId, 'error' => $error->getMessage()]);

                throw $error;
            }
        }, self::ATTEMPTS);
    }
}
