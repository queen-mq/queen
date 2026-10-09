<?php

namespace Queen\Tests\Integration;

use Queen\Exceptions\HttpException;
use Queen\Locks;

/**
 * Locks, semaphores, `check` and guarded transactions against a live broker.
 *
 * The names are fixed, as everywhere in this tree, and the purge below is
 * what makes that safe: a permit is a KV row in the namespace `queen-locks`
 * with the key `<name>#<slot>`, so cleaning up is deleting those rows — which
 * is also the claim the feature rests on, exercised on every run.
 *
 * What only a real broker can show: one holder; a call sent again by its
 * owner is the same permit; an expired lock goes to the next handle with a
 * higher token and the old handle's guarded step pushes nothing; a semaphore
 * never grants more than its limit.
 */
class LocksIntegrationTest extends IntegrationTestCase
{
    private const LOCKS = ['php-lock-one', 'php-lock-retry', 'php-lock-expiry', 'php-lock-keepalive', 'php-sem'];
    private const QUEUES = ['php-lock-one-q', 'php-lock-expiry-q'];
    private const SEM_LIMIT = 3;

    protected function setUp(): void
    {
        parent::setUp();
        $this->purgeLocks();
        $this->checksAreServed();
    }

    protected function tearDown(): void
    {
        $this->purgeLocks();
        parent::tearDown();
    }

    private function purgeLocks(): void
    {
        foreach (self::LOCKS as $name) {
            for ($slot = 0; $slot < 4; $slot++) {
                try {
                    $this->queen->kv()->delete(Locks::NAMESPACE, "{$name}#{$slot}");
                } catch (\Throwable) {
                    // Never taken, or the surface is paused: not this method's verdict.
                }
            }
        }
        foreach (self::QUEUES as $queue) {
            try {
                $this->queen->queue($queue)->delete()->execute();
            } catch (\Throwable) {
            }
        }
    }

    /** A broker alone raises its cluster version to 5 on its first tick; `check` needs it. */
    private function checksAreServed(): void
    {
        for ($i = 0; $i < 100; $i++) {
            try {
                $this->queen->kv()->check(self::NS_STATE, 'marker', ['expect' => 0]);
                return;
            } catch (HttpException $e) {
                if ($e->statusCode !== 503) {
                    throw $e;
                }
                usleep(100_000);
            }
        }
        $this->fail('the broker never served a check (cluster version below 5?)');
    }

    private function drain(string $queue): array
    {
        $seen = [];
        while (true) {
            $messages = $this->queen->queue($queue)->batch(50)->wait(false)->pop();
            if ($messages === []) {
                return $seen;
            }
            foreach ($messages as $message) {
                $seen[] = $message['data'];
            }
            $this->queen->ack($messages, true);
        }
    }

    public function testALockHasOneHolderAndItsGuardedStepCommits(): void
    {
        $a = $this->queen->lock('php-lock-one', ['ttlSeconds' => 30]);
        $b = $this->queen->lock('php-lock-one', ['ttlSeconds' => 30]);

        $this->assertTrue($a->acquire());
        $this->assertFalse($b->acquire(), 'a second handle acquired a held lock');
        $token = $a->token();

        $who = $this->queen->locks()->get('php-lock-one');
        $this->assertTrue($who['held']);
        $this->assertSame($a->owner(), $who['holders'][0]['owner']);
        $this->assertSame($token, $who['holders'][0]['token']);
        $this->assertNotEmpty($who['holders'][0]['expiresAt']);

        // The permit is a KV row and nothing else.
        $row = $this->queen->kv()->get(Locks::NAMESPACE, 'php-lock-one#0');
        $this->assertTrue($row['found']);
        $this->assertSame($token, $row['version']);
        $this->assertSame($a->owner(), $row['value']['owner']);

        $result = $this->queen->transaction()
            ->guard($a)
            ->queue('php-lock-one-q')->push([['data' => ['step' => 1]]])
            ->commit();
        $this->assertTrue($result['success']);
        $this->assertSame([['step' => 1]], $this->drain('php-lock-one-q'));

        $this->assertTrue($a->release());
        $this->assertTrue($b->acquire(), 'free after its release');
        $this->assertGreaterThan($token, $b->token(), 'a later holder, a higher token');
        $b->release();
    }

    public function testACallSentAgainByItsOwnerIsTheSamePermit(): void
    {
        $locks = $this->queen->locks();
        $first = $locks->acquire('php-lock-retry', ['ttlSeconds' => 30, 'owner' => 'me']);
        $again = $locks->acquire('php-lock-retry', ['ttlSeconds' => 30, 'owner' => 'me']);

        $this->assertTrue($first['acquired']);
        $this->assertTrue($again['acquired']);
        $this->assertTrue($again['already']);
        $this->assertSame($first['token'], $again['token'], 'the same permit, not a new one');

        $renewed = $locks->renew('php-lock-retry', $first['token'], ['ttlSeconds' => 30, 'owner' => 'me']);
        // The renew's answer is lost; the old token is sent again.
        $resent = $locks->renew('php-lock-retry', $first['token'], ['ttlSeconds' => 30, 'owner' => 'me']);
        $this->assertTrue($renewed['renewed']);
        $this->assertTrue($resent['renewed'], 'carried through for its owner');
        $this->assertGreaterThan($renewed['token'], $resent['token']);

        $stranger = $locks->renew('php-lock-retry', $first['token'], ['ttlSeconds' => 30, 'owner' => 'somebody-else']);
        $this->assertFalse($stranger['renewed']);
        $this->assertSame('lost', $stranger['reason']);
        $this->assertSame('me', $stranger['holders'][0]['owner']);

        $this->assertTrue($locks->release('php-lock-retry', $resent['token'])['released']);
    }

    public function testAnExpiredLockIsTakenOverAndTheOldHolderCommitsNothing(): void
    {
        // The old holder never calls keepAlive(): it is "paused" for longer
        // than its lease.
        $old = $this->queen->lock('php-lock-expiry', ['ttlSeconds' => 1]);
        $next = $this->queen->lock('php-lock-expiry', ['ttlSeconds' => 30]);

        $this->assertTrue($old->acquire());
        $oldToken = $old->token();
        $staleGuard = $old->guard();
        usleep(1_300_000);
        $this->assertFalse($old->held(), 'past its lifetime a handle does not claim to hold');

        $this->assertTrue($next->acquire(), 'an expired lock is free for the next holder');
        $this->assertGreaterThan($oldToken, $next->token());

        // The old holder wakes up and sends the step it was about to send.
        $stale = $this->queen->transaction()
            ->kv($staleGuard['ns'])->check($staleGuard['key'], ['expect' => $staleGuard['expect'], 'required' => true])
            ->queue('php-lock-expiry-q')->push([['data' => ['from' => 'old']]])
            ->commit();
        $this->assertFalse($stale['success']);
        $this->assertSame('kv_precondition', $stale['reason']);
        $this->assertSame($next->owner(), $stale['value']['owner']);

        $ok = $this->queen->transaction()
            ->guard($next)
            ->queue('php-lock-expiry-q')->push([['data' => ['from' => 'next']]])
            ->commit();
        $this->assertTrue($ok['success']);
        $this->assertSame([['from' => 'next']], $this->drain('php-lock-expiry-q'), "only the new holder's message exists");
        $next->release();
    }

    public function testKeepAliveCarriesALockPastItsFirstLifetime(): void
    {
        $lock = $this->queen->lock('php-lock-keepalive', ['ttlSeconds' => 2]);
        $this->assertTrue($lock->acquire());
        $first = $lock->token();

        // Work for longer than the lifetime, with a checkpoint every 250 ms.
        $end = microtime(true) + 3.0;
        while (microtime(true) < $end) {
            $this->assertTrue($lock->keepAlive(), 'the lock was lost at a checkpoint');
            usleep(250_000);
        }

        $this->assertTrue($lock->held());
        $this->assertGreaterThan($first, $lock->token(), 'it renewed along the way');
        $who = $this->queen->locks()->get('php-lock-keepalive');
        $this->assertSame($lock->owner(), $who['holders'][0]['owner']);
        $this->assertTrue($lock->release());
    }

    public function testASemaphoreNeverGrantsMoreThanItsLimit(): void
    {
        $permits = [];
        for ($i = 0; $i < 6; $i++) {
            $permits[] = $this->queen->semaphore('php-sem', self::SEM_LIMIT, ['ttlSeconds' => 30]);
        }

        $holders = array_values(array_filter($permits, static fn($p) => $p->acquire()));
        $this->assertCount(self::SEM_LIMIT, $holders, 'one at a time, exactly the limit is granted');

        $slots = array_map(static fn($p) => $p->slot(), $holders);
        sort($slots);
        $this->assertSame([0, 1, 2], $slots, 'one holder per slot');
        $this->assertCount(self::SEM_LIMIT, $this->queen->locks()->get('php-sem')['holders']);

        // One leaves; the next caller gets exactly that slot.
        $freed = $holders[0]->slot();
        $freedToken = $holders[0]->token();
        $this->assertTrue($holders[0]->release());
        $extra = $this->queen->semaphore('php-sem', self::SEM_LIMIT, ['ttlSeconds' => 30]);
        $this->assertTrue($extra->acquire());
        $this->assertSame($freed, $extra->slot());
        $this->assertGreaterThan($freedToken, $extra->token());

        // A holder that asks again is answered the permit it has.
        $again = $this->queen->locks()->acquire('php-sem', [
            'ttlSeconds' => 30, 'limit' => self::SEM_LIMIT, 'owner' => $holders[1]->owner(),
        ]);
        $this->assertTrue($again['acquired']);
        $this->assertTrue($again['already']);
        $this->assertSame($holders[1]->slot(), $again['slot']);

        foreach ([$extra, $holders[1], $holders[2]] as $permit) {
            $permit->release();
        }
        $this->assertFalse($this->queen->locks()->get('php-sem')['held']);
    }
}
