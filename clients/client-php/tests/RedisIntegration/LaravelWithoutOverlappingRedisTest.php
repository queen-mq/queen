<?php

namespace Queen\Tests\RedisIntegration;

use Orchestra\Testbench\TestCase;
use Queen\Tests\Support\BuildsQueenJobs;
use Queen\Tests\Support\OverlappingQueenJob;
use Redis;
use RuntimeException;
use Throwable;

final class LaravelWithoutOverlappingRedisTest extends TestCase
{
    use BuildsQueenJobs;

    private string $cachePrefix;

    private string $signalDirectory;

    private string $redisHost;

    private int $redisPort;

    protected function setUp(): void
    {
        $suffix = bin2hex(random_bytes(8));
        $this->cachePrefix = 'queen_without_overlap_' . $suffix . ':';
        $this->signalDirectory = sys_get_temp_dir() . '/queen-without-overlap-' . $suffix;
        $this->redisHost = (string) (getenv('QUEEN_LARAVEL_TEST_REDIS_HOST') ?: '127.0.0.1');
        $this->redisPort = (int) (getenv('QUEEN_LARAVEL_TEST_REDIS_PORT') ?: 6379);
        parent::setUp();
        OverlappingQueenJob::reset();
    }

    protected function tearDown(): void
    {
        OverlappingQueenJob::reset();
        foreach (['entered', 'release', 'child-error'] as $file) {
            @unlink($this->signalDirectory . '/' . $file);
        }
        @rmdir($this->signalDirectory);
        parent::tearDown();
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.redis.client', 'phpredis');
        $app['config']->set('database.redis.options', [
            'cluster' => 'redis',
            'prefix' => '',
        ]);
        $app['config']->set('database.redis.queen_overlap', [
            'host' => $this->redisHost,
            'password' => null,
            'port' => $this->redisPort,
            'database' => 15,
            'read_timeout' => 2,
        ]);
        $app['config']->set('cache.default', 'queen_overlap_redis');
        $app['config']->set('cache.prefix', $this->cachePrefix);
        $app['config']->set('cache.stores.queen_overlap_redis', [
            'driver' => 'redis',
            'connection' => 'queen_overlap',
            'lock_connection' => 'queen_overlap',
        ]);
    }

    public function testOrphanedRedisLockBlocksAQueenContenderUntilItExpires(): void
    {
        if (getenv('QUEEN_LARAVEL_TEST_REDIS_HOST') === false) {
            $this->markTestSkipped('Set QUEEN_LARAVEL_TEST_REDIS_HOST to run the Redis process integration test.');
        }
        if (!class_exists(Redis::class)) {
            $this->fail('The Redis integration suite requires the phpredis extension.');
        }
        if (!function_exists('pcntl_fork') || !function_exists('posix_kill') || !defined('SIGKILL')) {
            $this->fail('The Redis integration suite requires pcntl, posix, and SIGKILL support.');
        }
        if (!mkdir($this->signalDirectory, 0700) && !is_dir($this->signalDirectory)) {
            throw new RuntimeException('Unable to create the overlap-lock signal directory.');
        }

        $enteredSignal = $this->signalDirectory . '/entered';
        $releaseSignal = $this->signalDirectory . '/release';
        $childError = $this->signalDirectory . '/child-error';
        $expiresAfter = 5;
        $holderCommand = new OverlappingQueenJob(
            'redis-holder',
            'subscription-42',
            releaseAfter: 1,
            expiresAfter: $expiresAfter,
            shared: true,
            enteredSignal: $enteredSignal,
            releaseSignal: $releaseSignal,
        );
        $lockKey = $this->cachePrefix . $holderCommand->middleware()[0]->getLockKey($holderCommand);
        $redis = $this->redisConnection();
        $redis->del($lockKey);
        $redis->close();

        [$holder] = $this->queenJob(
            $holderCommand,
            'job-redis-holder',
            $this->acknowledgementResponse(),
        );

        $childPid = pcntl_fork();
        $this->assertNotSame(-1, $childPid, 'Unable to fork the Redis lock-holder process.');

        if ($childPid === 0) {
            try {
                $holder->fire();
                file_put_contents($childError, 'The lock holder returned before SIGKILL.', LOCK_EX);
                exit(2);
            } catch (Throwable $exception) {
                file_put_contents($childError, $exception::class . ': ' . $exception->getMessage(), LOCK_EX);
                exit(1);
            }
        }

        $childReaped = false;
        $redis = null;
        try {
            $this->waitForSignal($enteredSignal, $childError, $childPid, 5.0);

            [$whileHeld, $whileHeldHandler] = $this->queenJob(
                new OverlappingQueenJob(
                    'while-holder-is-alive',
                    'subscription-42',
                    releaseAfter: 1,
                    expiresAfter: $expiresAfter,
                    shared: true,
                ),
                'job-while-held',
                $this->transactionResponse(),
            );
            $whileHeld->fire();

            $this->assertTrue($whileHeld->isReleased());
            $this->assertSame([], OverlappingQueenJob::$entered);
            $this->assertSame(
                ['/api/v1/pop/queue/emails', '/api/v1/transaction'],
                $this->requestPaths($whileHeldHandler),
            );

            $this->assertTrue(posix_kill($childPid, SIGKILL), 'Unable to SIGKILL the lock-holder process.');
            $waited = pcntl_waitpid($childPid, $status);
            $childReaped = true;
            $this->assertSame($childPid, $waited);
            $this->assertTrue(pcntl_wifsignaled($status));
            $this->assertSame(SIGKILL, pcntl_wtermsig($status));

            $redis = $this->redisConnection();
            $remainingTtl = $redis->pttl($lockKey);
            $this->assertGreaterThan(0, $remainingTtl, 'SIGKILL must leave the Redis lock until its TTL expires.');

            $deadline = microtime(true) + $expiresAfter + 2;
            while ($redis->pttl($lockKey) !== -2 && microtime(true) < $deadline) {
                usleep(20_000);
            }
            $this->assertSame(-2, $redis->pttl($lockKey), 'The orphaned Redis lock did not expire within its TTL budget.');

            [$afterExpiry, $afterExpiryHandler] = $this->queenJob(
                new OverlappingQueenJob(
                    'after-redis-expiry',
                    'subscription-42',
                    releaseAfter: 1,
                    expiresAfter: $expiresAfter,
                    shared: true,
                ),
                'job-after-redis-expiry',
                $this->acknowledgementResponse(),
            );
            $afterExpiry->fire();

            $this->assertFalse($afterExpiry->isReleased());
            $this->assertSame(['after-redis-expiry'], OverlappingQueenJob::$entered);
            $this->assertSame(['after-redis-expiry'], OverlappingQueenJob::$completed);
            $this->assertSame(
                ['/api/v1/pop/queue/emails', '/api/v1/ack'],
                $this->requestPaths($afterExpiryHandler),
            );
        } finally {
            if (!$childReaped) {
                $waited = pcntl_waitpid($childPid, $cleanupStatus, WNOHANG);
                if ($waited === 0) {
                    posix_kill($childPid, SIGKILL);
                    pcntl_waitpid($childPid, $cleanupStatus);
                }
            }
            if ($redis instanceof Redis) {
                $redis->del($lockKey);
                $redis->close();
            } else {
                $cleanup = $this->redisConnection();
                $cleanup->del($lockKey);
                $cleanup->close();
            }
        }
    }

    private function redisConnection(): Redis
    {
        $redis = new Redis();
        if (!$redis->connect($this->redisHost, $this->redisPort, 2.0)) {
            throw new RuntimeException("Unable to connect to Redis at {$this->redisHost}:{$this->redisPort}.");
        }
        if (!$redis->select(15)) {
            throw new RuntimeException('Unable to select the Redis integration-test database.');
        }

        return $redis;
    }

    private function waitForSignal(string $signal, string $error, int $childPid, float $timeout): void
    {
        $deadline = microtime(true) + $timeout;
        while (!is_file($signal)) {
            if (is_file($error)) {
                throw new RuntimeException('Lock-holder process failed: ' . trim((string) file_get_contents($error)));
            }
            if (microtime(true) >= $deadline) {
                throw new RuntimeException('Timed out waiting for the Redis lock-holder process.');
            }
            usleep(10_000);
        }
    }
}
