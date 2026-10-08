<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Exceptions\LockNotHeldException;
use Queen\Lock;
use Queen\Locks;
use Queen\Queen;
use Queen\Support\KvOp;
use Queen\Tests\Support\PlanHandler;

/**
 * Locks: the wire of POST /api/v1/locks, the `check` KV operation, and what
 * the lock handle does around them — asserted as the EXACT JSON body, no
 * broker.
 *
 * Same method and same reason as KvTest: the body is the contract, and a
 * misspelled field is a 400 nobody can diagnose from the client side.
 *
 * What lives in the CLIENT and nowhere else, and is pinned here:
 *   - acquire() answers a boolean;
 *   - the handle always sends its owner, the same one on every call;
 *   - a renew's NEW token replaces the old one for the guard and the release;
 *   - keepAlive() sends nothing until a third of the lifetime has passed;
 *   - a guard that loses is the verdict, and the handle then holds nothing;
 *   - a transaction that asked for a guard never goes out without one.
 */
class LocksTest extends TestCase
{
    // ===========================
    // check
    // ===========================

    public function testCheckShape(): void
    {
        $this->assertSame(
            '{"op":"check","ns":"orders","key":"k","expect":7}',
            json_encode(KvOp::check('orders', 'k', ['expect' => 7]))
        );
        // The guard a lock hands out is this operation, required.
        $this->assertSame(
            '{"op":"check","ns":"queen-locks","key":"job#0","expect":41,"required":true}',
            json_encode(KvOp::check('queen-locks', 'job#0', ['expect' => 41, 'required' => true]))
        );
        // expect:0 is "must not exist" and reaches the wire as 0.
        $this->assertSame(0, KvOp::check('orders', 'k', ['expect' => 0])['expect']);
    }

    public function testACheckWithNoExpectIsRefusedBeforeTheRequest(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('check needs `expect`');
        KvOp::check('orders', 'k');
    }

    public function testACheckTakesNoLifetime(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('unknown kv option `ttlSeconds` for check');
        KvOp::check('orders', 'k', ['expect' => 1, 'ttlSeconds' => 30]);
    }

    public function testCheckPostsToTheKvRouteAndAnswersTheElement(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [
                ['index' => 0, 'op' => 'check', 'applied' => true, 'key' => 'k', 'version' => 7],
            ]]],
        ]);
        $queen = $this->queenFor($handler);

        $held = $queen->kv()->check('orders', 'k', ['expect' => 7]);

        $this->assertSame('/api/v1/kv', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(
            '{"operations":[{"op":"check","ns":"orders","key":"k","expect":7}]}',
            (string) $handler->requests[0]->getBody()
        );
        $this->assertTrue($held['applied']);
        $this->assertArrayNotHasKey('value', $held, 'a held check hands back no value');
    }

    // ===========================
    // The four operations
    // ===========================

    public function testOperationShapes(): void
    {
        $this->assertSame(
            '{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"}',
            json_encode(Locks::acquireOp('daily-report', ['ttlSeconds' => 30, 'owner' => 'o']))
        );
        $this->assertSame(
            '{"op":"acquire","name":"gpu","ttlSeconds":60,"limit":4}',
            json_encode(Locks::acquireOp('gpu', ['ttlSeconds' => 60, 'limit' => 4]))
        );
        $this->assertSame(
            '{"op":"renew","name":"gpu","token":100,"ttlSeconds":60,"slot":2,"owner":"o"}',
            json_encode(Locks::renewOp('gpu', 100, ['ttlSeconds' => 60, 'slot' => 2, 'owner' => 'o']))
        );
        // Slot 0, the only slot a lock has, is the default and is not sent.
        $this->assertSame(
            '{"op":"renew","name":"job","token":100,"ttlSeconds":30}',
            json_encode(Locks::renewOp('job', 100, ['ttlSeconds' => 30, 'slot' => 0]))
        );
        $this->assertSame(
            '{"op":"release","name":"daily-report","token":101}',
            json_encode(Locks::releaseOp('daily-report', 101))
        );
        $this->assertSame(
            '{"op":"release","name":"gpu","token":9,"slot":3}',
            json_encode(Locks::releaseOp('gpu', 9, 3))
        );
        $this->assertSame('{"op":"get","name":"daily-report"}', json_encode(Locks::getOp('daily-report')));
    }

    /**
     * @return array<string, array{callable, string}>
     */
    public static function refusals(): array
    {
        return [
            'no lifetime' => [fn() => Locks::acquireOp('a'), 'needs a lifetime'],
            'a lifetime of zero' => [fn() => Locks::acquireOp('a', ['ttlSeconds' => 0]), 'needs a lifetime'],
            'a float lifetime' => [fn() => Locks::acquireOp('a', ['ttlSeconds' => 1.5]), 'needs a lifetime'],
            'forever' => [fn() => Locks::acquireOp('a', ['forever' => true]), 'unknown lock option `forever`'],
            'a misspelled ttl' => [fn() => Locks::acquireOp('a', ['ttl' => 30]), 'unknown lock option `ttl`'],
            'a # in the name' => [fn() => Locks::acquireOp('a#b', ['ttlSeconds' => 1]), "without '#'"],
            'an empty name' => [fn() => Locks::getOp(''), 'non-empty string'],
            'a control character' => [fn() => Locks::getOp("a\nb"), 'non-empty string'],
            'a limit of zero' => [fn() => Locks::acquireOp('a', ['ttlSeconds' => 1, 'limit' => 0]), 'limit is a whole number'],
            'an empty owner' => [fn() => Locks::acquireOp('a', ['ttlSeconds' => 1, 'owner' => '']), 'owner is a non-empty string'],
            'a renew with no token' => [fn() => Locks::renewOp('a', 0, ['ttlSeconds' => 1]), 'renew needs the token'],
            'a release with no token' => [fn() => Locks::releaseOp('a', 0), 'release needs the token'],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('refusals')]
    public function testWhatTheBrokerWouldRefuseIsRefusedBeforeTheRequest(callable $call, string $message): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
        $call();
    }

    public function testTheWireAnswersVerdictsAsFieldsNeverAsErrors(): void
    {
        $handler = new PlanHandler([
            self::granted('daily-report', 100),
            self::refused('daily-report'),
            self::renewed('daily-report', 101),
            self::released('daily-report'),
            ['status' => 200, 'json' => ['results' => [
                ['index' => 0, 'op' => 'get', 'name' => 'daily-report', 'held' => false, 'holders' => []],
            ]]],
        ]);
        $locks = $this->queenFor($handler)->locks();

        $a = $locks->acquire('daily-report', ['ttlSeconds' => 30, 'owner' => 'o']);
        $this->assertTrue($a['acquired']);
        $this->assertSame(100, $a['token']);
        $this->assertSame(self::guard('daily-report', 0, 100), $a['guard']);

        $b = $locks->acquire('daily-report', ['ttlSeconds' => 30, 'owner' => 'p']);
        $this->assertFalse($b['acquired'], 'held by somebody else is a verdict, not an exception');
        $this->assertSame('held', $b['reason']);

        $r = $locks->renew('daily-report', 100, ['ttlSeconds' => 30]);
        $this->assertSame(101, $r['token'], 'a renew answers a NEW token');
        $this->assertTrue($locks->release('daily-report', 101)['released']);
        $this->assertFalse($locks->get('daily-report')['held']);

        $this->assertSame('POST', $handler->requests[0]->getMethod());
        $this->assertSame('/api/v1/locks', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(
            '{"operations":[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"}]}',
            (string) $handler->requests[0]->getBody()
        );
    }

    public function testAShortAnswerIsRefusedRatherThanGuessed(): void
    {
        $handler = new PlanHandler([['status' => 200, 'json' => ['results' => []]]]);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('with 1 element');
        $this->queenFor($handler)->locks()->get('a');
    }

    // ===========================
    // The handle
    // ===========================

    public function testAcquireAnswersABooleanAndTheHandleNamesItsOwner(): void
    {
        $handler = new PlanHandler([self::refused('job'), self::granted('job', 100), self::released('job')]);
        $queen = $this->queenFor($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);

        $this->assertInstanceOf(Lock::class, $lock);
        $this->assertFalse($lock->held());
        $this->assertNull($lock->token());
        $this->assertFalse($lock->acquire(), 'held by somebody else is false');

        $sent = self::op($handler, 0);
        $this->assertSame(['op', 'name', 'ttlSeconds', 'owner'], array_keys($sent));
        $this->assertSame($lock->owner(), $sent['owner']);
        $this->assertMatchesRegularExpression('/^.+:\d+:[0-9a-f]{12}$/', $lock->owner());

        $this->assertTrue($lock->acquire());
        $this->assertSame($lock->owner(), self::op($handler, 1)['owner'], 'the same owner on the retry');
        $this->assertTrue($lock->held());
        $this->assertSame(100, $lock->token());
        $this->assertSame(0, $lock->slot());
        $this->assertSame(self::guard('job', 0, 100), $lock->guard());
        $this->assertGreaterThan(25.0, $lock->expiresIn());
        $this->assertTrue($lock->acquire(), 'already held: no call');
        $this->assertSame(2, $handler->count());

        $this->assertTrue($lock->release());
        $this->assertSame(['op' => 'release', 'name' => 'job', 'token' => 100], self::op($handler, 2));
        $this->assertFalse($lock->held());
        $this->assertFalse($lock->release(), 'nothing left to give back, and no call');
        $this->assertSame(3, $handler->count());

        $this->assertNotSame($lock->owner(), $queen->lock('job', ['ttlSeconds' => 30])->owner());
        $this->assertSame('cron-7', $queen->lock('job', ['ttlSeconds' => 30, 'owner' => 'cron-7'])->owner());
    }

    public function testAGuardOfAnUnheldLockThrows(): void
    {
        $lock = $this->queenFor(new PlanHandler())->lock('job', ['ttlSeconds' => 30]);
        $this->expectException(LockNotHeldException::class);
        $lock->guard();
    }

    public function testALockHasOnePermitAndASemaphoreSaysSo(): void
    {
        $queen = $this->queenFor(new PlanHandler());
        $this->assertSame(4, $queen->semaphore('gpu', 4, ['ttlSeconds' => 60])->limit());
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('semaphore');
        $queen->lock('gpu', ['ttlSeconds' => 60, 'limit' => 4]);
    }

    public function testASemaphoreHandleSendsItsLimitAndKeepsItsSlot(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['results' => [[
                'index' => 0, 'op' => 'acquire', 'name' => 'gpu', 'acquired' => true, 'slot' => 3,
                'token' => 9, 'owner' => 'o', 'guard' => self::guard('gpu', 3, 9),
            ]]]],
            self::released('gpu'),
        ]);
        $permit = $this->queenFor($handler)->semaphore('gpu', 4, ['ttlSeconds' => 60]);

        $this->assertTrue($permit->acquire());
        $this->assertSame(4, self::op($handler, 0)['limit']);
        $this->assertSame(3, $permit->slot());
        $permit->release();
        $this->assertSame(['op' => 'release', 'name' => 'gpu', 'token' => 9, 'slot' => 3], self::op($handler, 1));
    }

    public function testAcquireWithAWaitComesBackUntilThePermitIsFree(): void
    {
        $handler = new PlanHandler([self::refused('job'), self::refused('job'), self::granted('job', 5)]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 30, 'retryMinMs' => 5, 'retryMaxMs' => 10]);
        $this->assertTrue($lock->acquire(5.0));
        $this->assertSame(3, $handler->count());

        $handler = new PlanHandler([], self::refused('job'));
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 30, 'retryMinMs' => 5, 'retryMaxMs' => 10]);
        $this->assertFalse($lock->acquire(0.06), 'the wait ran out');
        $this->assertGreaterThanOrEqual(2, $handler->count());
    }

    public function testARenewPutsTheNewTokenInTheGuardAndInTheRelease(): void
    {
        $handler = new PlanHandler([self::granted('job', 100), self::renewed('job', 101), self::released('job')]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        $this->assertTrue($lock->renew());
        $this->assertSame(
            ['op' => 'renew', 'name' => 'job', 'token' => 100, 'ttlSeconds' => 30, 'owner' => $lock->owner()],
            self::op($handler, 1)
        );
        $this->assertSame(101, $lock->token());
        $this->assertSame(self::guard('job', 0, 101), $lock->guard());
        $lock->release();
        $this->assertSame(101, self::op($handler, 2)['token']);
    }

    public function testARefusedRenewIsALoss(): void
    {
        $handler = new PlanHandler([self::granted('job', 100), ['status' => 200, 'json' => ['results' => [[
            'index' => 0, 'op' => 'renew', 'name' => 'job', 'renewed' => false, 'reason' => 'lost',
            'slot' => 0, 'holders' => [['slot' => 0, 'owner' => 'other']],
        ]]]]]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        $this->assertFalse($lock->renew());
        $this->assertFalse($lock->held());
        $this->assertNull($lock->token());
        $this->assertFalse($lock->release(), 'nothing to give back, and no call');
        $this->assertSame(2, $handler->count());
    }

    /**
     * Nothing renews a lock in the background in PHP. keepAlive() is the
     * checkpoint call: free until a third of the lifetime has passed, then a
     * renew.
     */
    public function testKeepAliveRenewsOnlyOnceAThirdOfTheLifetimeHasPassed(): void
    {
        $handler = new PlanHandler([self::granted('job', 100), self::renewed('job', 101)]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 3]);
        $lock->acquire();

        $this->assertTrue($lock->keepAlive());
        $this->assertTrue($lock->keepAlive());
        $this->assertSame(1, $handler->count(), 'not due yet: nothing was sent');

        usleep(1_050_000);
        $this->assertTrue($lock->keepAlive());
        $this->assertSame(2, $handler->count(), 'a third of the lifetime passed: one renew');
        $this->assertSame(101, $lock->token());
    }

    public function testALifetimeThatRunsOutHereIsNoLongerHeld(): void
    {
        $handler = new PlanHandler([self::granted('job', 100)]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 1]);
        $lock->acquire();
        $this->assertTrue($lock->held());

        usleep(1_050_000);
        $this->assertFalse($lock->held(), 'past its deadline a handle does not claim to hold');
        $this->assertNull($lock->token());
        $this->assertFalse($lock->keepAlive(), 'and keepAlive says stop, without a call');
        $this->assertSame(1, $handler->count());
    }

    public function testRunReleasesWhateverTheCallableDoes(): void
    {
        $handler = new PlanHandler([
            self::granted('job', 100), self::released('job'),
            self::refused('job'),
            self::granted('job', 200), self::released('job'),
        ]);
        $lock = $this->queenFor($handler)->lock('job', ['ttlSeconds' => 30]);

        $this->assertSame(['acquired' => true, 'value' => 42], $lock->run(fn(Lock $l) => $l->held() ? 42 : 0));
        $this->assertSame(['acquired' => false], $lock->run(fn() => 'never runs'));

        try {
            $lock->run(function () {
                throw new \LogicException('boom');
            });
            $this->fail('the callable threw');
        } catch (\LogicException $e) {
            $this->assertSame('boom', $e->getMessage());
        }
        $this->assertSame('release', self::op($handler, 4)['op'], 'released although the callable threw');
        $this->assertFalse($lock->held());
    }

    // ===========================
    // guard($lock) on a transaction
    // ===========================

    public function testTheGuardIsTheFirstKvOpAtTheTokenHeldWhenCommitSends(): void
    {
        $handler = new PlanHandler([
            self::granted('job', 100),
            self::renewed('job', 101),
            ['status' => 200, 'json' => ['transactionId' => 't', 'success' => true, 'results' => []]],
        ]);
        $queen = $this->queenFor($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        $tx = $queen->transaction()
            ->guard($lock)
            ->kv('work')->put('state', ['n' => 1], ['forever' => true]);
        $lock->renew();                       // after the guard was asked for, before commit
        $result = $tx->commit();

        $this->assertTrue($result['success']);
        $this->assertSame('/api/v1/transaction', $handler->requests[2]->getUri()->getPath());
        $body = json_decode((string) $handler->requests[2]->getBody(), true);
        $this->assertSame([
            self::guard('job', 0, 101),
            ['op' => 'put', 'ns' => 'work', 'key' => 'state', 'value' => ['n' => 1], 'forever' => true],
        ], $body['kv']);
    }

    public function testAGuardThatLosesIsTheVerdictAndTheHandleHoldsNothing(): void
    {
        $handler = new PlanHandler([self::granted('job', 100), self::lostTo(2, 'version', ['owner' => 'somebody-else'], 250)]);
        $queen = $this->queenFor($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        // Two pushed items come first in the flat index space: the guard is 2.
        $result = $queen->transaction()
            ->guard($lock)
            ->queue('reports')->push([['data' => 1], ['data' => 2]])
            ->commit();

        $this->assertFalse($result['success'], 'returned, not thrown');
        $this->assertSame('kv_precondition', $result['reason']);
        $this->assertFalse($lock->held());
    }

    public function testAPreconditionThatIsNotTheGuardsLeavesTheLockAlone(): void
    {
        // kv = [guard, marker]: flat index 1 is the bundle's own gate.
        $handler = new PlanHandler([self::granted('job', 100), self::lostTo(1, 'exists', true, 77)]);
        $queen = $this->queenFor($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);
        $lock->acquire();

        $result = $queen->transaction()
            ->guard($lock)
            ->kv('idem')->putIfAbsent('order-1', true, ['ttlSeconds' => 3600, 'required' => true])
            ->commit();

        $this->assertFalse($result['success']);
        $this->assertTrue($lock->held(), 'the marker lost, not the lock');
    }

    public function testAStepThatAskedForAGuardNeverGoesOutWithoutOne(): void
    {
        $handler = new PlanHandler();
        $queen = $this->queenFor($handler);
        $lock = $queen->lock('job', ['ttlSeconds' => 30]);

        try {
            $queen->transaction()->guard($lock)->kv('w')->put('k', 1, ['forever' => true])->commit();
            $this->fail('an unheld lock cannot guard');
        } catch (LockNotHeldException) {
            $this->assertSame(0, $handler->count());
        }
    }

    public function testCheckRidesATransactionInTheKvArray(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['transactionId' => 't', 'success' => true, 'results' => []]],
        ]);
        $this->queenFor($handler)->transaction()
            ->kv('queen-locks')->check('job#0', ['expect' => 41, 'required' => true])
            ->kv('work')->put('state', 1, ['forever' => true])
            ->commit();

        $body = json_decode((string) $handler->requests[0]->getBody(), true);
        $this->assertSame(self::guard('job', 0, 41), $body['kv'][0]);
    }

    // ===========================
    // Helpers
    // ===========================

    private function queenFor(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);
    }

    private static function op(PlanHandler $handler, int $request): array
    {
        return json_decode((string) $handler->requests[$request]->getBody(), true)['operations'][0];
    }

    private static function guard(string $name, int $slot, int $token): array
    {
        return ['op' => 'check', 'ns' => 'queen-locks', 'key' => "{$name}#{$slot}", 'expect' => $token, 'required' => true];
    }

    private static function granted(string $name, int $token): array
    {
        return ['status' => 200, 'json' => ['results' => [[
            'index' => 0, 'op' => 'acquire', 'name' => $name, 'acquired' => true, 'slot' => 0,
            'token' => $token, 'owner' => 'o', 'guard' => self::guard($name, 0, $token),
        ]]]];
    }

    private static function refused(string $name): array
    {
        return ['status' => 200, 'json' => ['results' => [[
            'index' => 0, 'op' => 'acquire', 'name' => $name, 'acquired' => false, 'reason' => 'held',
            'holders' => [['slot' => 0, 'owner' => 'other']],
        ]]]];
    }

    private static function renewed(string $name, int $token): array
    {
        return ['status' => 200, 'json' => ['results' => [[
            'index' => 0, 'op' => 'renew', 'name' => $name, 'renewed' => true, 'slot' => 0,
            'token' => $token, 'guard' => self::guard($name, 0, $token),
        ]]]];
    }

    private static function released(string $name): array
    {
        return ['status' => 200, 'json' => ['results' => [[
            'index' => 0, 'op' => 'release', 'name' => $name, 'released' => true, 'slot' => 0,
        ]]]];
    }

    private static function lostTo(int $failedIndex, string $kvReason, mixed $value, int $version): array
    {
        return ['status' => 200, 'json' => [
            'transactionId' => 't', 'success' => false, 'reason' => 'kv_precondition', 'error' => 'QKV',
            'results' => [], 'ok' => false, 'failedIndex' => $failedIndex, 'kvReason' => $kvReason,
            'value' => $value, 'version' => $version,
        ]];
    }
}
