<?php

namespace Queen\Laravel\Supervisor;

use Queen\Queen;

/**
 * Copies the supervisor status document to the broker's key/value store, in
 * the chunked format described by RemoteStatusDocument.
 *
 * The local status.json stays the source of truth for this host: liveness,
 * the owner lock and every control command keep reading it. The published
 * copy exists only so a dashboard running somewhere else can show the
 * supervisor, and it is best effort by construction. A failed publish never
 * stops, slows beyond its budgeted timeout, or otherwise changes the
 * orchestration loop; it is reported once per failure streak.
 */
final class RemoteStatusPublisher
{
    private ?float $lastPublishedAt = null;

    private ?string $lastPublishedState = null;

    private bool $failing = false;

    public function __construct(
        private Queen $queen,
        private string $namespace,
        private string $key,
        private int $intervalSeconds,
        private int $ttlSeconds,
        private ?\Closure $output = null,
        private ?\Closure $clock = null,
    ) {
    }

    /**
     * Publish the document, at most once per interval unless the supervisor
     * state changed (starting, pausing, terminating) since the last publish.
     *
     * @param array<string, mixed> $document the status document as written locally
     */
    public function publish(array $document): void
    {
        $now = $this->now();
        $state = is_string($document['state'] ?? null) ? $document['state'] : null;
        if ($this->lastPublishedAt !== null
            && $state === $this->lastPublishedState
            && $now - $this->lastPublishedAt < $this->intervalSeconds) {
            return;
        }

        // The legacy nested pool map duplicates pool_status, which every
        // current reader prefers. Dropping it roughly halves what travels.
        unset($document['pools']);

        try {
            $operations = RemoteStatusDocument::operations(
                $document,
                $this->namespace,
                $this->key,
                $this->ttlSeconds,
                RemoteStatusDocument::newWriteId(),
            );
            // One batch is one broker transaction: a reader never sees a head
            // without all of its chunks.
            $response = $this->queen->kv()->batch($operations);
            $results = $response['results'] ?? null;
            if (!is_array($results) || count($results) !== count($operations)) {
                $reason = is_string($response['reason'] ?? null) ? $response['reason'] : 'unexpected response';
                throw new \RuntimeException("the broker did not apply the write ({$reason})");
            }
            foreach ($results as $result) {
                if (($result['applied'] ?? null) !== true) {
                    $reason = is_string($result['reason'] ?? null) ? $result['reason'] : 'not applied';
                    throw new \RuntimeException("the broker did not apply the write ({$reason})");
                }
            }
        } catch (\Throwable $exception) {
            // Retry at the next interval rather than on every loop iteration.
            $this->lastPublishedAt = $now;
            $this->lastPublishedState = $state;
            if (!$this->failing) {
                $this->failing = true;
                $this->emit(
                    "Queen supervisor remote status publish failed: {$exception->getMessage()}. "
                    . "Local supervision continues; the remote dashboard will show this supervisor as stale.\n",
                    'err',
                );
            }

            return;
        }

        $this->lastPublishedAt = $now;
        $this->lastPublishedState = $state;
        if ($this->failing) {
            $this->failing = false;
            $this->emit("Queen supervisor remote status publishing recovered.\n", 'out');
        }
    }

    private function now(): float
    {
        return $this->clock !== null ? (float) ($this->clock)() : microtime(true);
    }

    private function emit(string $buffer, string $type): void
    {
        if ($this->output !== null) {
            ($this->output)($buffer, $type);
        }
    }
}
