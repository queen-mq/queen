<?php

namespace Queen\Laravel\Events;

/**
 * The oldest job waiting on a queue for one consumer group has waited longer
 * than the threshold configured in `queen.waits`; see `queen:check-waits`.
 */
final class LongWaitDetected
{
    public function __construct(
        public readonly string $connection,
        public readonly string $queue,
        public readonly string $consumerGroup,
        public readonly int $seconds,
        public readonly int $threshold,
    ) {
    }
}
