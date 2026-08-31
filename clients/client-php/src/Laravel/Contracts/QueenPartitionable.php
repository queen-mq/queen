<?php

namespace Queen\Laravel\Contracts;

/**
 * Opt a Laravel job into Queen's per-entity ordering.
 *
 * Jobs that do not implement this contract are spread across a fixed set of
 * stripes. Returning the same key here routes related jobs to the same Queen
 * partition and therefore preserves their order.
 */
interface QueenPartitionable
{
    public function queenPartition(): string;
}
