<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\AdaptiveBatch;

/**
 * prefetch "auto": each worker sizes its next pop from how long its jobs
 * take, so a batch holds about TARGET_MILLIS of work.
 */
final class AdaptiveBatchTest extends TestCase
{
    private float $now = 1_000.0;

    public function testAQueueStartsAtOneJobAndGrowsAtMostTwofoldPerFullBatch(): void
    {
        $batch = $this->batch();
        $this->assertSame(1, $this->full($batch, 'emails'), 'nothing measured yet');

        $this->runJobs($batch, 'emails', 3, 10.0);

        $this->assertSame(2, $this->full($batch, 'emails'));
        $this->assertSame(4, $this->full($batch, 'emails'));
        $this->assertSame(8, $this->full($batch, 'emails'));
        $this->assertSame(16, $this->full($batch, 'emails'), 'the ceiling');
        $this->assertSame(16, $this->full($batch, 'emails'));
    }

    public function testAShortBatchMeansNoBacklogSoTheNextAsksForWhatTheQueueHad(): void
    {
        $batch = $this->batch();
        $this->runJobs($batch, 'emails', 3, 10.0);
        foreach ([2, 4, 8] as $expected) {
            $this->assertSame($expected, $this->full($batch, 'emails'));
        }

        $batch->popped('emails', 16, 3);

        // Bigger batches would only hold jobs that idle workers could run.
        $this->assertSame(3, $batch->size('emails'));
        $batch->popped('emails', 3, 1);
        $this->assertSame(1, $batch->size('emails'));
        $batch->popped('emails', 1, 1);
        $this->assertSame(2, $batch->size('emails'), 'a full batch may grow again');
    }

    public function testAnEmptyPopFallsBackToOneJob(): void
    {
        $batch = $this->batch();
        $this->runJobs($batch, 'emails', 3, 10.0);
        $this->full($batch, 'emails');
        $this->full($batch, 'emails');

        $batch->popped('emails', 4, 0);

        $this->assertSame(1, $batch->size('emails'));
    }

    public function testABatchHoldsAboutTheTargetOfWork(): void
    {
        $batch = $this->batch();
        $this->runJobs($batch, 'reports', 4, 100.0);

        $this->assertSame(2, $this->full($batch, 'reports'), '250 ms of 100 ms jobs');
        $this->assertSame(2, $this->full($batch, 'reports'));
    }

    public function testLongJobsKeepOneJobPerPop(): void
    {
        $batch = $this->batch();
        $this->runJobs($batch, 'imports', 3, 1_500.0);

        $this->assertSame(1, $batch->size('imports'));
    }

    public function testASlowerJobShrinksTheNextBatchAtOnce(): void
    {
        $batch = $this->batch();
        $this->runJobs($batch, 'emails', 6, 5.0);
        for ($step = 0; $step < 5; ++$step) {
            $this->full($batch, 'emails');
        }
        $this->assertSame(16, $this->full($batch, 'emails'));

        $this->runJobs($batch, 'emails', 6, 400.0);

        $this->assertSame(1, $batch->size('emails'));
    }

    public function testOnlyTheJobIsMeasuredNotTheWaitForTheNextOne(): void
    {
        $batch = $this->batch();
        $batch->handedOut('emails');
        $this->now += 10.0;
        $batch->popping();
        // An empty long poll, then the worker's sleep.
        $this->now += 3_000.0;
        $batch->popping();
        $batch->handedOut('emails');
        $this->now += 10.0;
        $batch->popping();

        $this->assertSame(2, $batch->size('emails'), 'the 3 s wait was not a job');
    }

    public function testEachQueueIsMeasuredOnItsOwn(): void
    {
        $batch = $this->batch();
        $batch->handedOut('fast');
        $this->now += 5.0;
        $batch->popping();
        $batch->handedOut('slow');
        $this->now += 2_000.0;
        $batch->popping();

        $this->assertSame(2, $batch->size('fast'));
        $this->assertSame(1, $batch->size('slow'));
    }

    public function testTheCeilingIsHonoured(): void
    {
        $batch = new AdaptiveBatch(ceiling: 3, clock: fn (): float => $this->now);
        $this->runJobs($batch, 'emails', 3, 1.0);
        $this->full($batch, 'emails');
        $this->full($batch, 'emails');

        $this->assertSame(3, $this->full($batch, 'emails'));
    }

    /** Ask for the next batch and get all of it, as from a backlog. */
    private function full(AdaptiveBatch $batch, string $queue): int
    {
        $size = $batch->size($queue);
        $batch->popped($queue, $size, $size);

        return $size;
    }

    private function batch(): AdaptiveBatch
    {
        return new AdaptiveBatch(clock: fn (): float => $this->now);
    }

    /** $count jobs of $queue that each run $millis, as pop() sees them. */
    private function runJobs(AdaptiveBatch $batch, string $queue, int $count, float $millis): void
    {
        for ($index = 0; $index < $count; ++$index) {
            $batch->handedOut($queue);
            $this->now += $millis;
            $batch->popping();
        }
    }
}
