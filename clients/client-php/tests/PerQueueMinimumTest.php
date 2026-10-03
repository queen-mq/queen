<?php

namespace Queen\Tests;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\AutoScaler;
use Queen\Laravel\Supervisor\SupervisorConfiguration;

/**
 * min_processes_per_queue keeps every queue of an auto pool warm, like
 * Horizon's per-queue minProcesses. The Rust engine asserts the same cases
 * (`every_queue_keeps_its_minimum_like_horizon`).
 */
final class PerQueueMinimumTest extends TestCase
{
    public function testAnIdleQueueKeepsItsMinimum(): void
    {
        $this->assertSame(['high' => 2, 'default' => 2], (new AutoScaler())->desired($this->options(), ['high' => 0, 'default' => 0]));
        $this->assertSame(
            ['high' => 2, 'default' => 2],
            (new AutoScaler())->desired($this->options(), ['high' => 0, 'default' => 0], [], 1, 2),
        );
    }

    public function testTheBusyQueueGetsTheRestOfTheTarget(): void
    {
        $this->assertSame(['high' => 8, 'default' => 2], (new AutoScaler())->desired($this->options(), ['high' => 95, 'default' => 0]));
    }

    public function testTheFirstPollWithoutDepthStillKeepsTheMinimum(): void
    {
        $desired = (new AutoScaler())->desired($this->options(), ['high' => 1, 'default' => 1]);

        $this->assertGreaterThanOrEqual(2, $desired['high']);
        $this->assertGreaterThanOrEqual(2, $desired['default']);
    }

    public function testTheResolverExportsItOnlyWhenSet(): void
    {
        $unset = SupervisorConfiguration::resolve($this->queen([]), '/app');
        $this->assertArrayNotHasKey('min_processes_per_queue', $unset['supervisors']['default']);

        $set = SupervisorConfiguration::resolve($this->queen(['min_processes_per_queue' => '2']), '/app');
        $this->assertSame(2, $set['supervisors']['default']['min_processes_per_queue']);
    }

    public function testItNeedsAutoBalanceAndRoomInMaxProcesses(): void
    {
        foreach ([
            ['min_processes_per_queue' => 1, 'balance' => 'simple', 'processes' => 4],
            ['min_processes_per_queue' => 1, 'balance' => 'off'],
            ['min_processes_per_queue' => 6, 'max_processes' => 10],
            ['min_processes_per_queue' => -1],
        ] as $options) {
            try {
                SupervisorConfiguration::resolve($this->queen($options), '/app');
                $this->fail('Invalid per-queue minimum accepted: ' . json_encode($options));
            } catch (InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    /** @return array<string, mixed> */
    private function options(): array
    {
        return [
            'queues' => ['high', 'default'],
            'balance' => 'auto',
            'strategy' => 'size',
            'processes' => 10,
            'min_processes' => 0,
            'max_processes' => 10,
            'min_processes_per_queue' => 2,
            'target_jobs_per_process' => 10,
            'target_clear_seconds' => 60.0,
            'default_runtime_seconds' => 1.0,
        ];
    }

    /**
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    private function queen(array $options): array
    {
        return [
            'url' => 'http://queen.test:6632',
            'supervisor' => ['supervisors' => ['default' => ['queues' => ['high', 'default'], ...$options]]],
        ];
    }
}
