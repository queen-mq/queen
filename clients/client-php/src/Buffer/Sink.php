<?php

namespace Queen\Buffer;

/**
 * WHERE a buffered batch goes, and in WHAT shape (EPHEMERAL_QUEUES.md §4.1).
 *
 * The buffer machinery — the maxSize bound that refuses to grow, the inline
 * flush PHP needs in place of a background flusher, a failed batch back at the
 * FRONT and retried until it lands or its deadline expires — is about ordering,
 * occupancy and loss. None of that is durable-specific, and none of it is worth
 * writing twice. So the drain takes a SINK instead of a hardcoded POST.
 *
 * format() receives the queue and the partition because the two storage classes
 * disagree about where that identity lives on the wire, and that disagreement is
 * the entire reason this class exists:
 *
 *   - the DURABLE push wire repeats {queue, partition} on EVERY item, so the
 *     envelope is just {items} and the sink ignores both arguments;
 *   - the EPHEMERAL push wire hoists them to the envelope —
 *     {queue, partition?, messages:[{payload}...]} — so the batch elements
 *     carry nothing but their payload.
 *
 * Sink::durable() IS TODAY'S REQUEST, BYTE FOR BYTE. It is what a buffer created
 * without a destination drains into, which is every caller that existed before
 * ephemeral queues did, and tests/DurableSinkPinTest.php exists for no other
 * reason than to fail if that ever stops being true.
 */
final class Sink
{
    public const DURABLE_PATH = '/api/v1/push';
    public const EPHEMERAL_PATH = '/api/v1/ephemeral/push';

    private static ?self $durable = null;
    private static ?self $ephemeral = null;

    private function __construct(
        public readonly string $name,
        public readonly string $path,
        private readonly \Closure $formatter,
    ) {
    }

    /** The durable push wire: identity per item, envelope carries only the batch. */
    public static function durable(): self
    {
        return self::$durable ??= new self(
            'durable',
            self::DURABLE_PATH,
            fn(?string $queue, ?string $partition, array $batch): array => ['items' => $batch],
        );
    }

    /** The ephemeral push wire (§3.1): identity on the envelope. */
    public static function ephemeral(): self
    {
        return self::$ephemeral ??= new self(
            'ephemeral',
            self::EPHEMERAL_PATH,
            function (?string $queue, ?string $partition, array $batch): array {
                $body = ['queue' => $queue];
                // Omitted, never defaulted client-side: which partition an
                // ephemeral push without one lands on is the broker's rule, and
                // inventing a 'Default' here would take that decision away from
                // it in a way the caller never asked for.
                if ($partition !== null) {
                    $body['partition'] = $partition;
                }
                $body['messages'] = $batch;

                return $body;
            },
        );
    }

    /** The request body for one batch bound for (queue, partition). */
    public function format(?string $queue, ?string $partition, array $batch): array
    {
        return ($this->formatter)($queue, $partition, $batch);
    }
}
