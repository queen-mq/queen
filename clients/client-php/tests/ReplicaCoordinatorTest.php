<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\ReplicaCoordinator;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * The key layout and the scope hash are shared with the Rust engine
 * (supervisor/src/coordination.rs): replicas of both engines coordinate.
 */
final class ReplicaCoordinatorTest extends TestCase
{
    private const SELF = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';

    private const OTHER = '000000000000000018da146e6dc7d0d900000001';

    private float $now = 1000.0;

    /** @var list<array{0: string, 1: string}> */
    private array $output = [];

    public function testTheScopeNamesTheConsumerGroupAndTheQueueSet(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high', 'default']);

        // The same vector is asserted by the Rust engine.
        $this->assertSame('60e032fdac129da0', $scope);
        $this->assertNotSame($scope, ReplicaCoordinator::scope(['http://other.test:6632'], 'laravel', ['high', 'default']));
        $this->assertSame($scope, ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['default', 'high']));
        $this->assertNotSame($scope, ReplicaCoordinator::scope(['http://queen.test:6632'], 'emails', ['default', 'high']));
        $this->assertNotSame($scope, ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']));
    }

    public function testAHeartbeatRenewsThisReplicaAndListsTheScopeInOneCall(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $handler = new PlanHandler([$this->answer([[self::OTHER, self::SELF]], $scope)]);

        $this->coordinator($handler)->heartbeat([$scope, $scope]);

        $this->assertSame(1, $handler->count());
        $this->assertSame([
            [
                'op' => 'put',
                'ns' => 'queen-supervisor',
                'key' => "coordination/v1/{$scope}/" . self::SELF,
                'value' => ['instance_id' => self::SELF, 'hostname' => 'pod-b'],
                'ttlSeconds' => 30,
            ],
            [
                'op' => 'getPrefix',
                'ns' => 'queen-supervisor',
                'prefix' => "coordination/v1/{$scope}/",
                'limit' => ReplicaCoordinator::MEMBER_LIMIT,
                'keysOnly' => true,
            ],
        ], $this->operations($handler, 0));
    }

    public function testThePositionIsTheRankAmongTheSortedLiveReplicas(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $coordinator = $this->coordinator(new PlanHandler([$this->answer([[self::SELF, self::OTHER]], $scope)]));

        $this->assertSame([0, 1], $coordinator->position($scope));
        $coordinator->heartbeat([$scope]);

        $this->assertSame([1, 2], $coordinator->position($scope));
    }

    public function testThisReplicaCountsEvenBeforeItsOwnKeyIsListed(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $coordinator = $this->coordinator(new PlanHandler([$this->answer([[self::OTHER]], $scope)]));

        $coordinator->heartbeat([$scope]);

        $this->assertSame([1, 2], $coordinator->position($scope));
    }

    public function testForeignKeysUnderTheScopeAreIgnored(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $prefix = "coordination/v1/{$scope}/";
        $handler = new PlanHandler([['status' => 200, 'json' => ['results' => [
            ['applied' => true],
            ['rows' => [
                ['key' => $prefix . 'not-an-instance'],
                ['key' => $prefix . self::OTHER . '/nested'],
                ['key' => 'elsewhere/' . self::OTHER],
                ['value' => 'no key'],
            ], 'truncated' => false],
        ]]]]);
        $coordinator = $this->coordinator($handler);

        $coordinator->heartbeat([$scope]);

        $this->assertSame([0, 1], $coordinator->position($scope));
    }

    public function testManyPoolsAreSpreadOverCallsTheBrokerAccepts(): void
    {
        $scopes = array_map(fn (int $i): string => ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ["queue-{$i}"]), range(1, 5));
        $handler = new PlanHandler([
            $this->answer(array_fill(0, 4, [self::SELF]), ...array_slice($scopes, 0, 4)),
            $this->answer([[self::SELF, self::OTHER]], $scopes[4]),
        ]);
        $coordinator = $this->coordinator($handler);

        $coordinator->heartbeat($scopes);

        $this->assertSame(2, $handler->count());
        $this->assertCount(8, $this->operations($handler, 0));
        $this->assertCount(2, $this->operations($handler, 1));
        $this->assertSame([1, 2], $coordinator->position($scopes[4]));
    }

    public function testABrokerOutageKeepsTheLastViewUntilTheTtlThenSizesAlone(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $handler = new PlanHandler(
            [$this->answer([[self::OTHER, self::SELF]], $scope)],
            ['status' => 503, 'json' => ['error' => 'unavailable']],
        );
        $coordinator = $this->coordinator($handler);
        $coordinator->heartbeat([$scope]);

        $this->now += 10;
        $coordinator->heartbeat([$scope]);
        $this->now += 10;
        $coordinator->heartbeat([$scope]);
        $this->assertSame([1, 2], $coordinator->position($scope));
        $this->assertCount(1, $this->output);
        $this->assertSame('err', $this->output[0][1]);
        $this->assertStringContainsString('replica coordination failed', $this->output[0][0]);

        $this->assertTrue($coordinator->hasView($scope));
        $this->now += 11;
        $this->assertSame([0, 1], $coordinator->position($scope));
        // Without a view an event-driven step waits for the next heartbeat.
        $this->assertFalse($coordinator->hasView($scope));
    }

    public function testRecoveryIsReported(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $coordinator = $this->coordinator(new PlanHandler([
            ['status' => 200, 'json' => ['ok' => false, 'reason' => 'kv_precondition']],
            $this->answer([[self::SELF]], $scope),
        ]));

        $coordinator->heartbeat([$scope]);
        $coordinator->heartbeat([$scope]);

        $this->assertCount(2, $this->output);
        $this->assertStringContainsString('recovered', $this->output[1][0]);
    }

    public function testLeavingDeletesThisReplicaAndForgetsTheView(): void
    {
        $scope = ReplicaCoordinator::scope(['http://queen.test:6632'], 'laravel', ['high']);
        $handler = new PlanHandler([
            $this->answer([[self::OTHER, self::SELF]], $scope),
            ['status' => 200, 'json' => ['results' => [['applied' => true]]]],
        ]);
        $coordinator = $this->coordinator($handler);
        $coordinator->heartbeat([$scope]);
        $this->assertTrue($coordinator->hasView($scope));

        $coordinator->leave([$scope]);
        $this->assertFalse($coordinator->hasView($scope));

        $this->assertSame([[
            'op' => 'delete',
            'ns' => 'queen-supervisor',
            'key' => "coordination/v1/{$scope}/" . self::SELF,
        ]], $this->operations($handler, 1));
        $this->assertSame([0, 1], $coordinator->position($scope));
    }

    public function testOnlyAnEngineInstanceIdCanJoin(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new ReplicaCoordinator($this->queen(new PlanHandler()), 'queen-supervisor', 30, '../other');
    }

    private function coordinator(PlanHandler $handler): ReplicaCoordinator
    {
        return new ReplicaCoordinator(
            $this->queen($handler),
            'queen-supervisor',
            30,
            self::SELF,
            'pod-b',
            function (string $buffer, string $type): void {
                $this->output[] = [$buffer, $type];
            },
            fn (): float => $this->now,
        );
    }

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create($handler),
        ]);
    }

    /**
     * The broker's answer to one heartbeat call: per scope, the renewal and
     * the listing of the given instances.
     *
     * @param list<list<string>> $members
     */
    private function answer(array $members, string ...$scopes): array
    {
        $results = [];
        foreach ($scopes as $index => $scope) {
            $results[] = ['applied' => true];
            $results[] = [
                'rows' => array_map(fn (string $id): array => ['key' => "coordination/v1/{$scope}/{$id}"], $members[$index]),
                'truncated' => false,
                'nextAfter' => null,
            ];
        }

        return ['status' => 200, 'json' => ['results' => $results]];
    }

    /** @return list<array<string, mixed>> */
    private function operations(PlanHandler $handler, int $request): array
    {
        return json_decode((string) $handler->requests[$request]->getBody(), true)['operations'];
    }
}
