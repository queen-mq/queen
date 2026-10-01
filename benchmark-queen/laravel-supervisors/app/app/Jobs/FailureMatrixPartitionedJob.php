<?php

namespace App\Jobs;

use Queen\Laravel\Contracts\QueenPartitionable;

/**
 * A failure-matrix job in a named Queen partition. One pop leases the jobs of
 * a partition together, so with prefetch a worker runs them one after another
 * from its batch. Other queue drivers ignore the partition.
 */
final class FailureMatrixPartitionedJob extends FailureMatrixJob implements QueenPartitionable
{
    public function __construct(public string $partition, mixed ...$job)
    {
        parent::__construct(...$job);
    }

    public function queenPartition(): string
    {
        return $this->partition;
    }
}
