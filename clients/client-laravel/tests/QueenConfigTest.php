<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Support\Defaults;
use Queen\Tests\Support\PlanHandler;

class QueenConfigTest extends TestCase
{
    public function testConstructWithSingleUrl(): void
    {
        $queen = new Queen('http://localhost:6632');

        // Should be able to create queue builders
        $builder = $queen->queue('test');
        $this->assertInstanceOf(\Queen\Builders\QueueBuilder::class, $builder);
    }

    public function testConstructWithUrlArray(): void
    {
        $queen = new Queen(['http://a:6632', 'http://b:6632']);
        $builder = $queen->queue('test');
        $this->assertInstanceOf(\Queen\Builders\QueueBuilder::class, $builder);
    }

    public function testConstructWithConfigArray(): void
    {
        $queen = new Queen([
            'urls' => ['http://a:6632', 'http://b:6632'],
            'bearerToken' => 'test-token',
            'timeoutMillis' => 5000,
            'loadBalancingStrategy' => 'round-robin',
        ]);

        $builder = $queen->queue('test');
        $this->assertInstanceOf(\Queen\Builders\QueueBuilder::class, $builder);
    }

    public function testConstructWithSingleUrlConfig(): void
    {
        $queen = new Queen([
            'url' => 'http://localhost:6632',
        ]);

        $builder = $queen->queue('test');
        $this->assertInstanceOf(\Queen\Builders\QueueBuilder::class, $builder);
    }

    public function testConstructWithNoUrlThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Must provide urls or url');

        new Queen(['bearerToken' => 'test']);
    }

    public function testInvalidUrlsAndRetryBudgetsFailFast(): void
    {
        foreach ([
            ['urls' => []],
            ['url' => 'queen.test:6632'],
            ['url' => 'http://queen.test:6632', 'retryAttempts' => 0],
            ['url' => 'http://queen.test:6632', 'timeoutMillis' => 0],
            ['url' => 'http://queen.test:6632', 'loadBalancingStrategy' => 'random'],
        ] as $config) {
            try {
                new Queen($config);
                $this->fail('Invalid Queen client configuration was accepted.');
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testUnsafeHeadersCredentialsAndRetry429TyposFailFast(): void
    {
        foreach ([
            ['url' => 'http://user:secret@queen.test:6632'],
            ['url' => 'http://queen.test:6632?redirect=elsewhere'],
            ['url' => 'http://queen.test:6632#fragment'],
            ['url' => 'http://queen.test:6632', 'bearerToken' => "token\r\nInjected: yes"],
            ['url' => 'http://queen.test:6632', 'bearerToken' => 'token with spaces'],
            ['url' => 'http://queen.test:6632', 'headers' => ["X-Queen\nInjected" => 'yes']],
            ['url' => 'http://queen.test:6632', 'headers' => ['X-Queen' => "ok\r\nInjected: yes"]],
            ['url' => 'http://queen.test:6632', 'retry429' => ['baseMillis' => 5]],
            ['url' => 'http://queen.test:6632', 'retry429' => ['maxAttempts' => -1]],
            ['url' => 'http://queen.test:6632', 'retry429' => ['capMs' => 1.5]],
            ['url' => 'http://queen.test:6632', 'timeoutMillis' => '999999999999999999999999'],
            ['url' => 'http://queen.test:6632', 'headers' => ['Bad,Header' => 'value']],
        ] as $config) {
            try {
                new Queen($config);
                $this->fail('Unsafe or misspelled Queen client configuration was accepted.');
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testHeaderListsAndNumericRetry429StringsAreNormalized(): void
    {
        $queen = new Queen([
            'url' => 'https://queen.test/base',
            'headers' => ['X-Queen-Tenant' => ['one', 'two']],
            'retry429' => ['maxAttempts' => '3', 'baseMs' => '0', 'capMs' => '50'],
        ]);

        $this->assertInstanceOf(\Queen\Builders\QueueBuilder::class, $queen->queue('test'));
    }

    public function testAdminIsSingleton(): void
    {
        $queen = new Queen('http://localhost:6632');

        $admin1 = $queen->admin();
        $admin2 = $queen->admin();

        $this->assertSame($admin1, $admin2);
    }

    public function testTransactionBuilderCreated(): void
    {
        $queen = new Queen('http://localhost:6632');
        $tx = $queen->transaction();
        $this->assertInstanceOf(\Queen\Builders\TransactionBuilder::class, $tx);
    }

    public function testBufferStatsInitiallyEmpty(): void
    {
        $queen = new Queen('http://localhost:6632');
        $stats = $queen->getBufferStats();

        $this->assertSame(0, $stats['activeBuffers']);
        $this->assertSame(0, $stats['totalBufferedMessages']);
        $this->assertSame(0, $stats['flushesPerformed']);
    }

    public function testCloseDoesNotThrow(): void
    {
        $queen = new Queen('http://localhost:6632');
        $queen->close();

        // Should be safe to call close even with no active buffers
        $this->assertTrue(true);
    }

    public function testRenewPassesTheLeaseHorizonAndRequiresBrokerSuccessEvidence(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => [
                'success' => true,
                'renewed' => 2,
                'newExpiresAt' => '2026-08-29T07:00:00.000Z',
            ]],
            ['status' => 200, 'json' => [
                'success' => false,
                'renewed' => 0,
                'newExpiresAt' => null,
            ]],
        ]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $queen->renew('lease/with path', 481);
        $this->assertTrue($result['success']);
        $this->assertSame('2026-08-29T07:00:00.000Z', $result['newExpiresAt']);
        $this->assertSame('/api/v1/lease/lease%2Fwith%20path/extend', $handler->requests[0]->getUri()->getPath());
        $this->assertSame(['seconds' => 481], json_decode((string) $handler->requests[0]->getBody(), true));

        $rejected = $queen->renew('expired-lease', 481);
        $this->assertFalse($rejected['success']);
        $this->assertStringContainsString('could not verify', $rejected['error']);
    }

    public function testRenewRejectsInvalidLeaseHorizons(): void
    {
        $queen = new Queen('http://queen.test:6632');
        foreach ([0, -1, 2_147_483_648] as $seconds) {
            try {
                $queen->renew('lease-1', $seconds);
                $this->fail('An invalid lease renewal horizon was accepted.');
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testRenewRequiresAPositiveAuthoritativeCountAndValidatesExpiryMetadata(): void
    {
        $handler = new PlanHandler([
            ['status' => 200, 'json' => [
                'success' => true,
                'newExpiresAt' => '2026-08-29T07:00:00.000Z',
            ]],
            ['status' => 200, 'json' => [
                'success' => true,
                'renewed' => 0,
                'newExpiresAt' => '2026-08-29T07:00:00.000Z',
            ]],
            ['status' => 200, 'json' => [
                'success' => true,
                'renewed' => 1,
                'newExpiresAt' => 'not-a-timestamp',
            ]],
            ['status' => 200, 'json' => [
                'success' => true,
                'renewed' => 1,
                'newExpiresAt' => '2026-02-30T07:00:00Z',
            ]],
            ['status' => 200, 'json' => [
                'success' => true,
                'renewed' => 1,
            ]],
        ]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);

        foreach (['missing-count', 'zero-count', 'malformed-expiry', 'invalid-calendar-expiry'] as $leaseId) {
            $result = $queen->renew($leaseId, 60);
            $this->assertFalse($result['success']);
            $this->assertStringContainsString('could not verify', $result['error']);
        }

        $withoutOptionalExpiry = $queen->renew('positive-count', 60);
        $this->assertTrue($withoutOptionalExpiry['success']);
        $this->assertNull($withoutOptionalExpiry['newExpiresAt']);
    }
}
