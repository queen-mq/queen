<?php

namespace Queen\Tests;

use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Queue\QueenQueue;

/**
 * A supervisor starts a worker with its pool's settings in the environment:
 * QUEEN_LARAVEL_CONNECTION names the connection it works, and
 * QUEEN_LARAVEL_CONSUMER_GROUP, _RETRY_AFTER and _BLOCK_FOR are that
 * connection's. Another Queen connection the same worker resolves, to
 * dispatch onto it from a job, keeps its own configuration.
 */
final class LaravelWorkerConnectionScopeTest extends TestCase
{
    private const VARIABLES = [
        'QUEEN_LARAVEL_CONNECTION',
        'QUEEN_LARAVEL_CONSUMER_GROUP',
        'QUEEN_LARAVEL_RETRY_AFTER',
        'QUEEN_LARAVEL_BLOCK_FOR',
    ];

    /** @var array<string, string|false> */
    private array $saved = [];

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.default', 'sync');
        $app['config']->set('queue.connections.queen-a', [
            'driver' => 'queen', 'url' => 'http://a:6632', 'consumer_group' => 'group-a', 'retry_after' => 90,
        ]);
        $app['config']->set('queue.connections.queen-b', [
            'driver' => 'queen', 'url' => 'http://b:6632', 'consumer_group' => 'group-b', 'retry_after' => 600, 'block_for' => 5,
        ]);
    }

    protected function setUp(): void
    {
        parent::setUp();

        foreach (self::VARIABLES as $name) {
            $this->saved[$name] = getenv($name);
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->saved as $name => $value) {
            putenv($value === false ? $name : "{$name}={$value}");
        }

        parent::tearDown();
    }

    public function testThePoolsSettingsApplyToTheWorkedConnectionAlone(): void
    {
        $this->environment(['QUEEN_LARAVEL_CONNECTION' => 'queen-a', 'QUEEN_LARAVEL_CONSUMER_GROUP' => 'pool-a', 'QUEEN_LARAVEL_RETRY_AFTER' => '120', 'QUEEN_LARAVEL_BLOCK_FOR' => '0']);

        $worked = $this->app['queue']->connection('queen-a');
        $other = $this->app['queue']->connection('queen-b');

        $this->assertSame(['pool-a', 120, 0], $this->settings($worked));
        $this->assertSame(['group-b', 600, 5], $this->settings($other));
    }

    /** A supervisor that names no connection: every connection takes the settings, as before. */
    public function testWithoutTheConnectionNameTheSettingsApplyToEveryConnection(): void
    {
        $this->environment(['QUEEN_LARAVEL_CONNECTION' => null, 'QUEEN_LARAVEL_CONSUMER_GROUP' => 'pool-a', 'QUEEN_LARAVEL_RETRY_AFTER' => '120', 'QUEEN_LARAVEL_BLOCK_FOR' => '0']);

        $this->assertSame(['pool-a', 120, 0], $this->settings($this->app['queue']->connection('queen-b')));
    }

    /** No supervisor at all: the configuration. */
    public function testWithoutASupervisorEveryConnectionKeepsItsConfiguration(): void
    {
        $this->environment(array_fill_keys(self::VARIABLES, null));

        $this->assertSame(['group-a', 90, 0], $this->settings($this->app['queue']->connection('queen-a')));
        $this->assertSame(['group-b', 600, 5], $this->settings($this->app['queue']->connection('queen-b')));
    }

    /** @param array<string, ?string> $variables */
    private function environment(array $variables): void
    {
        foreach ($variables as $name => $value) {
            putenv($value === null ? $name : "{$name}={$value}");
        }
    }

    /** @return array{0: string, 1: int, 2: int} consumer group, retry_after, block_for */
    private function settings(QueenQueue $queue): array
    {
        $read = fn (string $property): mixed => (new \ReflectionProperty(QueenQueue::class, $property))->getValue($queue);

        return [$read('consumerGroup'), $read('retryAfter'), $read('blockFor')];
    }
}
