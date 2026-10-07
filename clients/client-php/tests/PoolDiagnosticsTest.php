<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\PoolDiagnostics;

final class PoolDiagnosticsTest extends TestCase
{
    private function diagnose(array $pool = [], array $instance = []): array
    {
        return PoolDiagnostics::forInstance(array_replace([
            'availability' => 'live', 'state' => 'running',
            'process_budget' => ['valid' => true, 'available' => 0],
            'pools' => [array_replace([
                'supervisor' => 'main', 'queue' => 'orders', 'processes' => 4, 'desired' => 4,
                'counts_available' => true, 'draining' => 0, 'depth' => 120, 'depth_available' => true,
                'ready' => true, 'healthy' => true, 'restart_state' => 'closed',
                'process_cost_per_worker' => 2,
            ], $pool)],
        ], $instance))[0];
    }

    public function testBacklogAloneDoesNotImplyCapacityFailure(): void
    {
        $row = $this->diagnose(['depth' => 100000]);
        self::assertSame('At desired capacity', $row['label']);
        self::assertSame('', $row['tone']);
    }

    public function testMissingCountsCannotDiagnoseZeroWorkers(): void
    {
        $row = $this->diagnose(['counts_available' => false, 'processes' => 0, 'desired' => 0]);
        self::assertSame('Incomplete telemetry', $row['label']);
    }

    public function testPendingWorkWithoutWorkersIsActionableEvenAtZeroTarget(): void
    {
        $row = $this->diagnose(['processes' => 0, 'desired' => 0]);
        self::assertSame('Pending work, no workers', $row['label']);
        self::assertStringContainsString('target is zero', $row['next']);
        self::assertSame('danger', $row['tone']);
    }

    public function testBudgetFindingRequiresValidatedHeadroomAndKnownWorkerCost(): void
    {
        self::assertSame('No process headroom', $this->diagnose(['desired' => 6])['label']);
        self::assertSame('Below desired capacity', $this->diagnose(['desired' => 6, 'process_cost_per_worker' => null])['label']);
        self::assertSame('Below desired capacity', $this->diagnose(['desired' => 6], ['process_budget' => ['valid' => false, 'available' => 0]])['label']);
        self::assertSame('Below desired capacity', $this->diagnose(['desired' => 6], ['process_budget' => ['valid' => true, 'available' => 2]])['label']);
    }

    public function testRestartEvidenceTakesPriorityOverAnExhaustedBudget(): void
    {
        self::assertSame('Restart backoff', $this->diagnose(['desired' => 6, 'restart_state' => 'backoff'])['label']);
        self::assertSame('Restart circuit open', $this->diagnose(['restart_state' => 'open'])['label']);
    }

    public function testStaleAndPausedInstancesNeverReportCurrentPoolHealth(): void
    {
        self::assertSame('Current state unknown', $this->diagnose([], ['availability' => 'stale'])['label']);
        self::assertSame('Supervisor paused', $this->diagnose(['restart_state' => 'open'], ['state' => 'paused'])['label']);
        self::assertSame('Supervisor terminating', $this->diagnose([], ['state' => 'terminating'])['label']);
    }

    public function testUnknownDepthAndUnconfirmedReadinessStayExplicit(): void
    {
        self::assertSame('Incomplete telemetry', $this->diagnose(['depth_available' => false])['label']);
        self::assertSame('Readiness not confirmed', $this->diagnose(['ready' => false])['label']);
        self::assertSame('Workers draining', $this->diagnose(['draining' => 2])['label']);
    }
}
