<?php

namespace Queen\Tests;

use Illuminate\Queue\QueueManager;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Commands\SuperviseCommand;
use Queen\Laravel\Commands\SupervisorConfigCommand;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Laravel\Queue\SyncedFailedJobProvider;

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
            ->assertSuccessful();

        $this->assertTrue(class_exists(SuperviseCommand::class));
        $this->assertTrue(class_exists(SupervisorConfigCommand::class));
    }

    public function testProviderSynchronizesLaravelFailedJobsWithQueenDlq(): void
    {
        $this->assertInstanceOf(SyncedFailedJobProvider::class, $this->app['queue.failer']);
    }
}
