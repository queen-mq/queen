<?php

namespace Queen\Tests;

use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use ReflectionMethod;

/**
 * fast_scale_up closes half of the gap every cycle. The Rust engine asserts
 * the same steps (`a_fast_burst_closes_half_of_the_gap_every_cycle`).
 */
final class FastScaleUpTest extends TestCase
{
    public function testABurstClosesHalfOfTheGapEveryCycle(): void
    {
        $fast = [...$this->options(), 'fast_scale_up' => true];

        $this->assertSame(1, $this->budget($this->options(), 1, 20));
        $this->assertSame(10, $this->budget($fast, 1, 20));
        $this->assertSame(5, $this->budget($fast, 11, 20));
        $this->assertSame(1, $this->budget($fast, 19, 20));
        // Scaling down keeps the configured step.
        $this->assertSame(1, $this->budget($fast, 20, 4));

        $cycles = 0;
        for ($active = 1; $active < 20; ++$cycles) {
            $active += min($this->budget($fast, $active, 20), 20 - $active);
        }
        $this->assertSame(5, $cycles);
    }

    public function testThePerQueueMinimumIsEstablishedInOneCycle(): void
    {
        $floor = [...$this->options(), 'min_processes' => 0, 'min_processes_per_queue' => 2];

        $this->assertSame(4, $this->budget($floor, 0, 4));
    }

    public function testTheResolverExportsItOnlyWhenEnabled(): void
    {
        $queen = fn (array $options): array => [
            'url' => 'http://queen.test:6632',
            'supervisor' => ['supervisors' => ['default' => ['queues' => ['high'], ...$options]]],
        ];

        $this->assertArrayNotHasKey('fast_scale_up', SupervisorConfiguration::resolve($queen([]), '/app')['supervisors']['default']);
        $this->assertTrue(SupervisorConfiguration::resolve($queen(['fast_scale_up' => true]), '/app')['supervisors']['default']['fast_scale_up']);
        $this->expectException(\InvalidArgumentException::class);
        SupervisorConfiguration::resolve($queen(['fast_scale_up' => 'yes']), '/app');
    }

    /** @param array<string, mixed> $options */
    private function budget(array $options, int $active, int $desired): int
    {
        $supervisor = new PhpSupervisor($this->createStub(QueueManager::class), ['state_directory' => sys_get_temp_dir() . '/unused']);

        return (new ReflectionMethod(PhpSupervisor::class, 'reconcileBudget'))->invoke($supervisor, $options, $active, $desired);
    }

    /** @return array<string, mixed> */
    private function options(): array
    {
        return [
            'queues' => ['high', 'default'],
            'balance' => 'auto',
            'min_processes' => 1,
            'max_processes' => 20,
            'balance_max_shift' => 1,
        ];
    }
}
