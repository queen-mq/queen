<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Queue\QueueManager;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Commands\SuperviseCommand;
use Queen\Laravel\Commands\SupervisorConfigCommand;
use Queen\Laravel\Commands\SupervisorInstallCommand;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Laravel\Queue\SyncedFailedJobProvider;
use Queen\Tests\Support\PlanHandler;

class LaravelServiceProviderTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    public function testProviderRegistersAUsableLaravelQueueConnection(): void
    {
        $config = $this->app['config']->get('queue.connections.queen');

        $this->assertSame('queen', $config['driver']);
        $this->assertSame('default', $config['queue']);
        $this->assertSame('laravel', $config['consumer_group']);

        $connection = $this->app->make(QueueManager::class)->connection('queen');

        $this->assertInstanceOf(QueenQueue::class, $connection);
        $this->assertSame('laravel', $connection->getConsumerGroup());
    }

    public function testExplicitConnectionOptionsOverridePackageDefaults(): void
    {
        $this->app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'queue' => 'priority',
            'consumer_group' => 'priority-workers',
        ]);

        $connection = $this->app->make(QueueManager::class)->connection('queen');

        $this->assertInstanceOf(QueenQueue::class, $connection);
        $this->assertSame('priority-workers', $connection->getConsumerGroup());
    }

    public function testProviderRegistersSupervisorCommands(): void
    {
        $this->artisan('list')
            ->expectsOutputToContain('queen:supervise')
            ->expectsOutputToContain('queen:supervisor-config')
            ->expectsOutputToContain('queen:supervisor-install')
            ->assertSuccessful();

        $this->assertTrue(class_exists(SuperviseCommand::class));
        $this->assertTrue(class_exists(SupervisorConfigCommand::class));
        $this->assertTrue(class_exists(SupervisorInstallCommand::class));
    }

    public function testProviderSynchronizesLaravelFailedJobsWithQueenDlq(): void
    {
        $this->assertInstanceOf(SyncedFailedJobProvider::class, $this->app['queue.failer']);
    }

    public function testProviderWiresTheAtomicRetryFenceIntoTheQueenConnection(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'queen-failed-retry-');
        $this->assertIsString($path);
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['success' => true]],
            ['status' => 201, 'json' => [['status' => 'queued']]],
        ]);

        $this->app['config']->set('queue.failed', [
            'driver' => 'file',
            'path' => $path,
            'limit' => 10,
        ]);
        $this->app['config']->set('queue.connections.queen', [
            'driver' => 'queen',
            'url' => 'http://queen.test:6632',
            'queue' => 'critical',
            'handler' => HandlerStack::create($handler),
            'retry_attempts' => 1,
        ]);
        $this->app->forgetInstance('queue.failer');

        try {
            $provider = $this->app['queue.failer'];
            $this->assertInstanceOf(SyncedFailedJobProvider::class, $provider);
            $oldPayload = json_encode([
                'uuid' => 'reused',
                '_queen' => [
                    'manual_retry' => 'retry-old',
                    'failed_source' => [
                        'partition_id' => 'partition-old',
                        'transaction_id' => 'transaction-old',
                    ],
                ],
            ], JSON_THROW_ON_ERROR);
            $provider->log('queen', 'critical', $oldPayload, new \RuntimeException('old'));

            $selected = $provider->find('reused');
            $this->assertNotNull($selected);
            $this->assertSame(
                'reused',
                $this->app['queue']->connection('queen')->pushRaw($selected->payload, 'critical'),
            );

            $newPayload = json_encode([
                'uuid' => 'reused',
                '_queen' => [
                    'manual_retry' => 'retry-new',
                    'failed_source' => [
                        'partition_id' => 'partition-new',
                        'transaction_id' => 'transaction-new',
                    ],
                ],
            ], JSON_THROW_ON_ERROR);
            $provider->log('queen', 'critical', $newPayload, new \RuntimeException('new'));

            $this->assertFalse($provider->forget('reused'));
            $current = $provider->find('reused');
            $this->assertNotNull($current);
            $this->assertSame(
                'retry-new',
                json_decode($current->payload, true, 512, JSON_THROW_ON_ERROR)['_queen']['manual_retry'],
            );
            $this->assertCount(2, $handler->requests);
        } finally {
            @unlink($path);
        }
    }

    public function testNativeSupervisorOfflineCommandFailsBeforeNetworkWithoutALocalManifest(): void
    {
        $this->artisan('queen:supervisor-install', ['--archive' => __FILE__])
            ->expectsOutputToContain('Offline --archive requires an explicit local --manifest.')
            ->assertFailed();
    }

    public function testFailedJobLockDefaultsExposeAConservativePerMutationBudget(): void
    {
        $this->assertNull($this->app['config']->get('queen.failed_jobs_lock_store'));
        $this->assertSame('queen:failed-jobs', $this->app['config']->get('queen.failed_jobs_lock_name'));
        $this->assertSame(600, $this->app['config']->get('queen.failed_jobs_lock_ttl'));
        $this->assertSame(600, $this->app['config']->get('queen.failed_jobs_lock_wait'));
    }

    public function testFailedJobMutationFailsClosedWhenConfiguredLockIsBusy(): void
    {
        $lockName = 'queen:test:failed-jobs:' . bin2hex(random_bytes(8));
        $this->app['config']->set('queen.failed_jobs_lock_name', $lockName);
        $this->app['config']->set('queen.failed_jobs_lock_ttl', '5');
        $this->app['config']->set('queen.failed_jobs_lock_wait', '0');
        $held = $this->app['cache']->lock($lockName, 5);
        $this->assertTrue($held->acquire());

        try {
            $provider = $this->app['queue.failer'];
            $this->assertInstanceOf(SyncedFailedJobProvider::class, $provider);
            $provider->forget('does-not-exist');
            $this->fail('A busy distributed lock must abort the failed-store mutation.');
        } catch (\RuntimeException $exception) {
            $this->assertSame(
                'Timed out acquiring the Queen failed-job cache lock; no index mutation was attempted.',
                $exception->getMessage(),
            );
            $this->assertInstanceOf(
                \Illuminate\Contracts\Cache\LockTimeoutException::class,
                $exception->getPrevious(),
            );
        } finally {
            $held->release();
        }
    }

    public function testInvalidFailedJobLockTtlFailsBeforeProviderDecoration(): void
    {
        $this->app['config']->set('queen.failed_jobs_lock_ttl', 0);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('queen.failed_jobs_lock_ttl must be an integer of at least 1.');

        $this->app['queue.failer'];
    }
}
