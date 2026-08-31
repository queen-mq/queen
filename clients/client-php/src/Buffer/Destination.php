<?php

namespace Queen\Buffer;

/**
 * What a buffer drains into: a Sink, plus the identity that sink formats for
 * (EPHEMERAL_QUEUES.md §4.1).
 *
 * ADDRESSES ARE NAMESPACED, and that is the other half of this class's job. A
 * buffer address is the key of the one-buffer-per-address registry, so an
 * ephemeral `orders` and a durable `orders` must not land on the same entry —
 * they are unrelated objects (§10 Q8) and a shared buffer would post one
 * family's messages to the other family's route. The `eph:` prefix is the same
 * namespacing the broker applies to its own queue keys (§3.2), for the same
 * reason.
 */
final class Destination
{
    public function __construct(
        public readonly Sink $sink,
        public readonly ?string $queue = null,
        public readonly ?string $partition = null,
    ) {
    }

    /**
     * The default for a buffer created without one, so a buffer that predates
     * sinks behaves exactly as it did before they existed.
     */
    public static function durable(): self
    {
        return new self(Sink::durable());
    }

    /** The ephemeral counterpart, bound to one (queue, partition). */
    public static function ephemeral(string $queue, ?string $partition = null): self
    {
        return new self(Sink::ephemeral(), $queue, $partition);
    }

    /**
     * The durable buffer address, unchanged: "queue/partition". Kept here next
     * to its ephemeral sibling so the two can be compared at a glance; the
     * durable push path builds the same string inline.
     */
    public static function durableAddress(string $queue, string $partition): string
    {
        return "{$queue}/{$partition}";
    }

    /**
     * The ephemeral buffer address: "eph:queue/partition", or "eph:queue" when
     * the caller named no partition — which is a DIFFERENT destination from any
     * named one, because the broker picks, and a buffer must not merge the two.
     *
     * Same ambiguity as the durable address (a queue named "a/b" collides with
     * ("a", "b")), inherited deliberately rather than fixed on one side only.
     */
    public static function ephemeralAddress(string $queue, ?string $partition = null): string
    {
        return $partition === null ? "eph:{$queue}" : "eph:{$queue}/{$partition}";
    }

    /** The request body for one batch, from this destination's own identity. */
    public function format(array $batch): array
    {
        return $this->sink->format($this->queue, $this->partition, $batch);
    }
}
