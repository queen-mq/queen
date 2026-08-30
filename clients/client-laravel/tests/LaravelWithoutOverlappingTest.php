<?php

namespace Queen\Tests;

use Fiber;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Support\Carbon;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Tests\Support\BuildsQueenJobs;
use Queen\Tests\Support\OverlappingQueenJob;

final class LaravelWithoutOverlappingTest extends TestCase
{
    use BuildsQueenJobs;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('cache.default', 'array');
        $app['config']->set('cache.stores.array', [
            'driver' => 'array',
            'serialize' => false,
        ]);
    }

    protected function setUp(): void
    {
        parent::setUp();
        OverlappingQueenJob::reset();
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();
        OverlappingQueenJob::reset();
        parent::tearDown();
    }

    public function testConcurrentQueenDeliveriesUseTheSharedLaravelOverlapLock(): void
    {
        [$first, $firstHandler] = $this->queenJob(
            new OverlappingQueenJob('first', 'account-42', suspend: true),
            'job-first',
            $this->acknowledgementResponse(),
        );
        [$contender, $contenderHandler] = $this->queenJob(
            new OverlappingQueenJob('contender', 'account-42', releaseAfter: 7),
            'job-contender',
            $this->transactionResponse(),
        );

        $firstWorker = new Fiber(static fn () => $first->fire());

        $this->assertSame('first', $firstWorker->start());
        $this->assertSame(['first'], OverlappingQueenJob::$entered);

        $contender->fire();

        $this->assertTrue($contender->isReleased());
        $this->assertSame(['first'], OverlappingQueenJob::$entered);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/transaction'],
            $this->requestPaths($contenderHandler),
        );

        $release = json_decode((string) $contenderHandler->requests[1]->getBody(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('ack', $release['operations'][0]['type']);
        $this->assertSame('completed', $release['operations'][0]['status']);
        $this->assertSame(['lease-job-contender'], $release['requiredLeases']);
        $this->assertSame(7_000, $release['timers'][0]['delayMs']);
        $releasedPayload = json_decode(
            base64_decode($release['timers'][0]['payload'], true),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $this->assertSame(1, $releasedPayload['_queen']['attempts']);

        $firstWorker->resume();

        $this->assertTrue($firstWorker->isTerminated());
        $this->assertSame(['first'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($firstHandler),
        );

        [$afterRelease, $afterReleaseHandler] = $this->queenJob(
            new OverlappingQueenJob('after-release', 'account-42'),
            'job-after-release',
            $this->acknowledgementResponse(),
        );
        $afterRelease->fire();

        $this->assertSame(['first', 'after-release'], OverlappingQueenJob::$entered);
        $this->assertSame(['first', 'after-release'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($afterReleaseHandler),
        );
    }

    public function testExpiredAbandonedLockLetsAQueenJobRun(): void
    {
        Carbon::setTestNow('2026-08-30 22:00:00 UTC');
        $crashed = new OverlappingQueenJob(
            'crashed-owner',
            'invoice-9',
            releaseAfter: 3,
            expiresAfter: 5,
        );
        $middleware = $crashed->middleware()[0];
        $cache = $this->app->make(CacheRepository::class);
        $orphanedLock = $cache->lock($middleware->getLockKey($crashed), 5);
        $this->assertTrue($orphanedLock->get());

        [$beforeExpiry, $beforeExpiryHandler] = $this->queenJob(
            new OverlappingQueenJob(
                'before-expiry',
                'invoice-9',
                releaseAfter: 3,
                expiresAfter: 5,
            ),
            'job-before-expiry',
            $this->transactionResponse(),
        );
        $beforeExpiry->fire();

        $this->assertTrue($beforeExpiry->isReleased());
        $this->assertSame([], OverlappingQueenJob::$entered);
        $release = json_decode((string) $beforeExpiryHandler->requests[1]->getBody(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame(3_000, $release['timers'][0]['delayMs']);

        Carbon::setTestNow(Carbon::now()->addSeconds(6));
        [$afterExpiry, $afterExpiryHandler] = $this->queenJob(
            new OverlappingQueenJob(
                'after-expiry',
                'invoice-9',
                releaseAfter: 3,
                expiresAfter: 5,
            ),
            'job-after-expiry',
            $this->acknowledgementResponse(),
        );
        $afterExpiry->fire();

        $this->assertSame(['after-expiry'], OverlappingQueenJob::$entered);
        $this->assertSame(['after-expiry'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($afterExpiryHandler),
        );
    }

    public function testExpireAfterDoesNotFenceAStillRunningQueenJob(): void
    {
        Carbon::setTestNow('2026-08-30 22:00:00 UTC');
        [$slow, $slowHandler] = $this->queenJob(
            new OverlappingQueenJob(
                'slow-owner',
                'export-12',
                expiresAfter: 5,
                suspend: true,
            ),
            'job-slow-owner',
            $this->acknowledgementResponse(),
        );
        $slowWorker = new Fiber(static fn () => $slow->fire());
        $this->assertSame('slow-owner', $slowWorker->start());

        Carbon::setTestNow(Carbon::now()->addSeconds(6));
        [$successor, $successorHandler] = $this->queenJob(
            new OverlappingQueenJob('successor', 'export-12', expiresAfter: 5),
            'job-successor',
            $this->acknowledgementResponse(),
        );
        $successor->fire();

        $this->assertSame(['slow-owner', 'successor'], OverlappingQueenJob::$entered);
        $this->assertSame(['successor'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($successorHandler),
        );

        $slowWorker->resume();

        $this->assertTrue($slowWorker->isTerminated());
        $this->assertSame(['successor', 'slow-owner'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($slowHandler),
        );
    }

    public function testSharedOverlapKeyProtectsDifferentQueenJobClasses(): void
    {
        $unsharedFirst = new FirstSharedOverlappingQueenTestJob('first-unshared', 'customer-17');
        $unsharedSecond = new SecondSharedOverlappingQueenTestJob('second-unshared', 'customer-17');
        $this->assertNotSame(
            $unsharedFirst->middleware()[0]->getLockKey($unsharedFirst),
            $unsharedSecond->middleware()[0]->getLockKey($unsharedSecond),
        );

        $sharedFirst = new FirstSharedOverlappingQueenTestJob(
            'first-class',
            'customer-17',
            suspend: true,
            shared: true,
        );
        $sharedSecond = new SecondSharedOverlappingQueenTestJob(
            'second-class',
            'customer-17',
            releaseAfter: 2,
            shared: true,
        );
        $this->assertSame(
            $sharedFirst->middleware()[0]->getLockKey($sharedFirst),
            $sharedSecond->middleware()[0]->getLockKey($sharedSecond),
        );

        [$first] = $this->queenJob(
            $sharedFirst,
            'job-first-class',
            $this->acknowledgementResponse(),
        );
        [$second, $secondHandler] = $this->queenJob(
            $sharedSecond,
            'job-second-class',
            $this->transactionResponse(),
        );

        $firstWorker = new Fiber(static fn () => $first->fire());
        $this->assertSame('first-class', $firstWorker->start());

        $second->fire();

        $this->assertTrue($second->isReleased());
        $this->assertSame(['first-class'], OverlappingQueenJob::$entered);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/transaction'],
            $this->requestPaths($secondHandler),
        );

        $firstWorker->resume();
        $this->assertTrue($firstWorker->isTerminated());
    }

    public function testOverlapLockIsReleasedWhenAQueenJobThrows(): void
    {
        [$failing, $failingHandler] = $this->queenJob(
            new OverlappingQueenJob(
                'failing',
                'report-31',
                failAfterEntering: true,
            ),
            'job-failing',
            $this->acknowledgementResponse(),
        );

        try {
            $failing->fire();
            $this->fail('The fixture job was expected to throw.');
        } catch (\RuntimeException $exception) {
            $this->assertSame('Intentional overlap-lock job failure.', $exception->getMessage());
        }
        $this->assertSame(
            ['/api/v1/pop/queue/emails'],
            $this->requestPaths($failingHandler),
            'A thrown job must not be acknowledged as completed.',
        );

        [$retry, $retryHandler] = $this->queenJob(
            new OverlappingQueenJob('retry-after-failure', 'report-31'),
            'job-retry-after-failure',
            $this->acknowledgementResponse(),
        );
        $retry->fire();

        $this->assertSame(['failing', 'retry-after-failure'], OverlappingQueenJob::$entered);
        $this->assertSame(['retry-after-failure'], OverlappingQueenJob::$completed);
        $this->assertSame(
            ['/api/v1/pop/queue/emails', '/api/v1/ack'],
            $this->requestPaths($retryHandler),
        );
    }
}

final class FirstSharedOverlappingQueenTestJob extends OverlappingQueenJob
{
}

final class SecondSharedOverlappingQueenTestJob extends OverlappingQueenJob
{
}
