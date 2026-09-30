<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\AutoScaler;

/**
 * Coordinated replicas split one fleet target. The Rust engine implements the
 * same rule (supervisor/src/main.rs `desired`), so these allocations are a
 * cross-engine contract.
 */
final class ReplicaScalingTest extends TestCase
{
    public function testReplicasSplitTheFleetTargetInsteadOfEachReachingIt(): void
    {
        $depths = ['high' => 95, 'default' => 5];

        $first = $this->desired($this->options(), $depths, 0, 2);
        $second = $this->desired($this->options(), $depths, 1, 2);

        $this->assertSame(5, array_sum($first));
        $this->assertSame(5, array_sum($second));
        $this->assertSame(10, array_sum($this->desired($this->options(), $depths)));
    }

    public function testTheRemainderGoesToTheLowestRanks(): void
    {
        $options = array_replace($this->options(), ['min_processes' => 0]);
        $depths = ['high' => 70, 'default' => 0];

        $this->assertSame(
            [3, 2, 2],
            array_map(fn (int $rank): int => array_sum($this->desired($options, $depths, $rank, 3)), [0, 1, 2]),
        );
    }

    public function testMinimumAndMaximumApplyToEveryReplica(): void
    {
        $small = $this->desired($this->options(), ['high' => 10, 'default' => 0], 1, 2);
        $this->assertSame(2, array_sum($small));

        $large = $this->desired($this->options(), ['high' => 400, 'default' => 0], 0, 2);
        $this->assertSame(10, array_sum($large));
    }

    public function testReplicasCoverDifferentQueuesWhenTheTargetIsSmall(): void
    {
        $options = array_replace($this->options(), [
            'queues' => ['a', 'b', 'c', 'd'],
            'min_processes' => 0,
        ]);
        $depths = ['a' => 1, 'b' => 1, 'c' => 1, 'd' => 1];

        // The fleet needs one worker per queue with backlog: four, two each.
        $this->assertSame(['a' => 1, 'b' => 1, 'c' => 0, 'd' => 0], $this->desired($options, $depths, 0, 2));
        $this->assertSame(['a' => 0, 'b' => 0, 'c' => 1, 'd' => 1], $this->desired($options, $depths, 1, 2));
    }

    public function testEveryQueueWithBacklogIsCoveredWhenTheMaximumCutsTheShare(): void
    {
        $options = array_replace($this->options(), [
            'queues' => ['q0', 'q1', 'q2', 'q3', 'q4'],
            'min_processes' => 0,
            'max_processes' => 3,
        ]);
        $depths = array_fill_keys($options['queues'], 60);

        $fleet = array_fill_keys($options['queues'], 0);
        foreach ([0, 1, 2] as $rank) {
            foreach ($this->desired($options, $depths, $rank, 3) as $queue => $workers) {
                $fleet[$queue] += $workers;
            }
            // The coverage does not move when the backlog grows.
            $this->assertSame(
                array_map(fn (int $workers): bool => $workers > 0, $this->desired($options, $depths, $rank, 3)),
                array_map(fn (int $workers): bool => $workers > 0, $this->desired($options, array_fill_keys($options['queues'], 61), $rank, 3)),
            );
        }

        $this->assertSame(9, array_sum($fleet));
        $this->assertNotContains(0, $fleet);
    }

    public function testASmallTargetStillCoversEveryQueueWithBacklog(): void
    {
        $options = array_replace($this->options(), [
            'queues' => ['q0', 'q1', 'q2', 'q3', 'q4'],
            'min_processes' => 0,
        ]);
        $depths = array_fill_keys($options['queues'], 1);

        $fleet = array_fill_keys($options['queues'], 0);
        foreach ([0, 1, 2] as $rank) {
            foreach ($this->desired($options, $depths, $rank, 3) as $queue => $workers) {
                $fleet[$queue] += $workers;
            }
        }

        $this->assertNotContains(0, $fleet);
    }

    public function testAnIdleFleetKeepsTheMinimumOnEveryReplica(): void
    {
        foreach ([0, 1, 2] as $rank) {
            $this->assertSame(2, array_sum($this->desired($this->options(), ['high' => 0, 'default' => 0], $rank, 3)));
        }
    }

    public function testFixedPoolsAreNotSplit(): void
    {
        $options = array_replace($this->options(), ['balance' => 'simple', 'processes' => 6]);

        $this->assertSame(['high' => 3, 'default' => 3], $this->desired($options, ['high' => 100, 'default' => 0], 1, 4));
    }

    public function testOrderedPriorityPutsTheShareOnTheFirstQueue(): void
    {
        $options = array_replace($this->options(), ['balance' => 'off', 'min_processes' => 0]);

        $this->assertSame(['high' => 3, 'default' => 0], $this->desired($options, ['high' => 30, 'default' => 30], 1, 2));
    }

    public function testNonFinitePressureSaturatesEveryReplica(): void
    {
        $options = array_replace($this->options(), [
            'strategy' => 'time',
            'target_clear_seconds' => 1.0,
        ]);

        $desired = (new AutoScaler())->desired(
            $options,
            ['high' => PHP_INT_MAX, 'default' => PHP_INT_MAX],
            ['high' => 1.0e308, 'default' => 1.0e308],
            1,
            2,
        );

        $this->assertSame(10, array_sum($desired));
    }

    public function testAnImpossibleReplicaPositionIsRefused(): void
    {
        foreach ([[0, 0], [2, 2], [-1, 3]] as [$rank, $count]) {
            try {
                $this->desired($this->options(), ['high' => 1, 'default' => 1], $rank, $count);
                $this->fail("Replica {$rank} of {$count} was accepted.");
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    /**
     * @param array<string, mixed> $options
     * @param array<string, int> $depths
     * @return array<string, int>
     */
    private function desired(array $options, array $depths, int $replica = 0, int $replicas = 1): array
    {
        return (new AutoScaler())->desired($options, $depths, [], $replica, $replicas);
    }

    /** @return array<string, mixed> */
    private function options(): array
    {
        return [
            'queues' => ['high', 'default'],
            'balance' => 'auto',
            'strategy' => 'size',
            'processes' => 10,
            'min_processes' => 2,
            'max_processes' => 10,
            'target_jobs_per_process' => 10,
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ];
    }
}
