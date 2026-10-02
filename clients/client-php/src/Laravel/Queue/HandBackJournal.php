<?php

namespace Queen\Laravel\Queue;

use RuntimeException;

/**
 * @internal What this worker owes for its leased batch if it crashes: the
 *           transaction the supervisor's lease service sends when the worker
 *           exits holding the lease, as shutdown() would have.
 *
 * Two files next to the lease socket, named by this process's PID:
 *
 *     hand-back-<pid>.plan   {"lease_id":..,"entries":[{"ack":..,"unstarted":..,"ran":..}, ..]}
 *     hand-back-<pid>.state  {"lease_id":..,"entries":"cur-.."}
 *
 * The plan holds each delivery's completed ACK and the two copies a hand-back
 * may push, once per batch. The state holds one code per entry: `-` nothing,
 * `c` the ACK alone, `u` the ACK and the unstarted copy, `r` the ACK and the
 * copy that counts the run. It is rewritten in place by one write of at most
 * a page, which a crash cannot tear, and every record of a batch has the same
 * length. The supervisor's `lease/hand_back.rs` reads them, and so does
 * owed() for the PHP lease helper.
 *
 * Any I/O failure removes both files and stops journaling for this process:
 * a journal the worker can no longer keep current must not be sent.
 */
final class HandBackJournal
{
    private const MAX_PLAN_BYTES = 16 * 1024 * 1024;

    private const MAX_STATE_BYTES = 4096;

    /** @var resource|null */
    private $state = null;

    /** The lease of the journaled batch; null when nothing is journaled. */
    private ?string $leaseId = null;

    private int $entries = 0;

    private ?string $written = null;

    private bool $failed = false;

    public function __construct(private string $pathPrefix)
    {
    }

    /**
     * What the journal at $pathPrefix owes for $leaseId, as the supervisor's
     * `lease/hand_back.rs` reads it: the operations to send, in the order of
     * the plan, and how many jobs they hand back. Null when it owes nothing.
     *
     * @return array{operations: list<array>, unstarted: int, running: int}|null
     * @throws RuntimeException when the journal does not hold together
     */
    public static function owed(string $pathPrefix, string $leaseId): ?array
    {
        $state = self::read("{$pathPrefix}.state", self::MAX_STATE_BYTES);
        // An empty state: a batch planned, nothing owed yet.
        if ($state === null || trim($state) === '') {
            return null;
        }
        $state = json_decode($state, true);
        if (!is_array($state) || !is_string($state['lease_id'] ?? null) || !is_string($state['entries'] ?? null)) {
            throw new RuntimeException('invalid hand-back state');
        }
        if ($state['lease_id'] !== $leaseId || trim($state['entries'], '-') === '') {
            return null;
        }
        $plan = self::read("{$pathPrefix}.plan", self::MAX_PLAN_BYTES)
            ?? throw new RuntimeException('the hand-back plan is missing');
        $plan = json_decode($plan, true);
        if (!is_array($plan) || !is_array($plan['entries'] ?? null) || !array_is_list($plan['entries'])) {
            throw new RuntimeException('invalid hand-back plan');
        }
        if (($plan['lease_id'] ?? null) !== $leaseId || count($plan['entries']) !== strlen($state['entries'])) {
            throw new RuntimeException('the hand-back plan does not match its state');
        }

        $operations = [];
        $unstarted = 0;
        $running = 0;
        foreach (str_split($state['entries']) as $index => $code) {
            if ($code === '-') {
                continue;
            }
            $entry = $plan['entries'][$index];
            $copy = match ($code) {
                'c' => null,
                'u' => $entry['unstarted'] ?? false,
                'r' => $entry['ran'] ?? false,
                default => throw new RuntimeException('invalid hand-back state'),
            };
            $ack = $entry['ack'] ?? null;
            if (!is_array($ack) || ($ack['type'] ?? null) !== 'ack' || ($ack['status'] ?? null) !== 'completed'
                || ($ack['leaseId'] ?? null) !== $leaseId) {
                throw new RuntimeException('a hand-back ACK does not name the lease');
            }
            $operations[] = $ack;
            if ($copy === null) {
                continue;
            }
            if (!is_array($copy) || ($copy['type'] ?? null) !== 'push') {
                throw new RuntimeException('a hand-back copy is not a push');
            }
            $operations[] = $copy;
            $code === 'u' ? $unstarted++ : $running++;
        }

        return ['operations' => $operations, 'unstarted' => $unstarted, 'running' => $running];
    }

    /**
     * Journal a new batch, owing nothing yet.
     *
     * @param list<array{ack: array, unstarted: array, ran: array}> $entries
     * @return bool whether the batch is journaled
     */
    public function plan(string $leaseId, array $entries): bool
    {
        if ($this->failed) {
            return false;
        }
        $this->leaseId = null;
        $this->written = null;
        try {
            $this->openState();
            // The previous batch's state goes first: it must never be read
            // against this plan.
            if (!ftruncate($this->state, 0)) {
                throw new RuntimeException('the state cannot be reset');
            }
            $this->entries = count($entries);
            if (strlen($this->record($leaseId, str_repeat('-', $this->entries))) > self::MAX_STATE_BYTES) {
                return false;
            }
            $plan = json_encode(
                ['lease_id' => $leaseId, 'entries' => $entries],
                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR,
            );
            if (strlen($plan) > self::MAX_PLAN_BYTES) {
                return false;
            }
            $temporary = $this->pathPrefix . '.plan.tmp';
            if (@file_put_contents($temporary, $plan) !== strlen($plan)
                || !@rename($temporary, $this->pathPrefix . '.plan')) {
                throw new RuntimeException('the plan cannot be written');
            }
        } catch (\Throwable $exception) {
            $this->fail($exception);
            return false;
        }
        $this->leaseId = $leaseId;

        return true;
    }

    /** What a crash would owe now, one code per entry of the plan. */
    public function owe(string $codes): void
    {
        if ($this->leaseId === null || strlen($codes) !== $this->entries) {
            return;
        }
        $record = $this->record($this->leaseId, $codes);
        if ($record === $this->written) {
            return;
        }
        if (fseek($this->state, 0) !== 0 || @fwrite($this->state, $record) !== strlen($record)) {
            $this->fail(new RuntimeException('the state cannot be written'));
            return;
        }
        $this->written = $record;
    }

    /** Owe nothing: the worker settles the batch itself, as at shutdown. */
    public function withdraw(): void
    {
        if ($this->leaseId === null) {
            return;
        }
        $this->owe(str_repeat('-', $this->entries));
        $this->leaseId = null;
    }

    /** Remove both files, on a clean exit. */
    public function discard(): void
    {
        $this->leaseId = null;
        $this->written = null;
        if (is_resource($this->state)) {
            @fclose($this->state);
        }
        $this->state = null;
        self::remove($this->pathPrefix);
    }

    /** Remove the journal at $pathPrefix, once nothing reads it again. */
    public static function remove(string $pathPrefix): void
    {
        foreach (['.state', '.plan', '.plan.tmp'] as $suffix) {
            @unlink($pathPrefix . $suffix);
        }
    }

    private static function read(string $path, int $limit): ?string
    {
        if (!is_file($path)) {
            return null;
        }
        $bytes = @file_get_contents($path, false, null, 0, $limit + 1);
        if (!is_string($bytes)) {
            throw new RuntimeException('the hand-back journal cannot be read');
        }
        if (strlen($bytes) > $limit) {
            throw new RuntimeException('the hand-back journal is too large');
        }

        return $bytes;
    }

    private function openState(): void
    {
        if (is_resource($this->state)) {
            return;
        }
        $state = @fopen($this->pathPrefix . '.state', 'c');
        if (!is_resource($state)) {
            throw new RuntimeException('the state cannot be opened');
        }
        // Each record must reach the file in the write that sends it.
        stream_set_write_buffer($state, 0);
        $this->state = $state;
    }

    private function record(string $leaseId, string $codes): string
    {
        return json_encode(
            ['lease_id' => $leaseId, 'entries' => $codes],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR,
        ) . "\n";
    }

    private function fail(\Throwable $exception): void
    {
        $this->discard();
        $this->failed = true;
        error_log('Queen Laravel stopped journaling the hand-back of prefetched jobs; '
            . 'a crash charges each one an attempt: ' . $exception->getMessage());
    }
}
