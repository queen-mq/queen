<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Notification;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Events\LongWaitDetected;
use Queen\Laravel\Monitoring\QueueWaits;
use Queen\Laravel\Notifications\LongWaitDetected as LongWaitNotification;
use Queen\Laravel\QueenServiceProvider;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class LaravelCheckWaitsTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('cache.default', 'array');
        $app['config']->set('queen.waits', ['queen:high' => 60]);
        $app['config']->set('queen.notifications', ['mail' => 'ops@example.test, oncall@example.test', 'throttle_minutes' => 5]);
        $app['config']->set('queen.supervisor.supervisors', [
            'default' => ['connection' => 'queen', 'consumer_group' => 'laravel', 'queues' => ['high', 'default']],
            'emails' => ['connection' => 'queen', 'consumer_group' => 'emails', 'queues' => ['high']],
        ]);
    }

    public function testALongWaitDispatchesTheEventAndMailsOncePerThrottleWindow(): void
    {
        Event::fake([LongWaitDetected::class]);
        Notification::fake();
        $this->broker(fn (string $group): int => $group === 'laravel' ? 95 : 10);

        $this->artisan('queen:check-waits')->assertSuccessful();
        $this->artisan('queen:check-waits')->assertSuccessful();

        Event::assertDispatchedTimes(LongWaitDetected::class, 1);
        Event::assertDispatched(fn (LongWaitDetected $wait): bool => $wait->queue === 'high'
            && $wait->consumerGroup === 'laravel'
            && $wait->seconds === 95
            && $wait->threshold === 60);
        Notification::assertSentOnDemandTimes(LongWaitNotification::class, 1);
        Notification::assertSentOnDemand(
            LongWaitNotification::class,
            fn ($notification, array $channels, $notifiable): bool => $notifiable->routes['mail'] === ['ops@example.test', 'oncall@example.test'],
        );
    }

    public function testAnAlertThatCouldNotBeSentIsTriedAgainOnTheNextRun(): void
    {
        Notification::fake();
        $this->broker(fn (string $group): int => $group === 'laravel' ? 95 : 10);
        $calls = 0;
        Event::listen(LongWaitDetected::class, function () use (&$calls): void {
            if (++$calls === 1) {
                throw new \RuntimeException('the alert route is down');
            }
        });

        $this->artisan('queen:check-waits')->assertFailed();
        $this->artisan('queen:check-waits')->assertSuccessful();

        $this->assertSame(2, $calls);
        Notification::assertSentOnDemandTimes(LongWaitNotification::class, 1);
    }

    public function testShortWaitsReportNothing(): void
    {
        Event::fake([LongWaitDetected::class]);
        Notification::fake();
        $this->broker(fn (): int => 12);

        $this->artisan('queen:check-waits')->assertSuccessful();

        Event::assertNotDispatched(LongWaitDetected::class);
        Notification::assertNothingSent();
    }

    public function testAnUnreachableBrokerFailsTheRun(): void
    {
        $this->app->bind(QueueWaits::class, fn () => new QueueWaits(new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'handler' => HandlerStack::create(new PlanHandler([], ['status' => 503, 'json' => []])),
        ])));

        $this->artisan('queen:check-waits')->assertFailed();
    }

    /** @param \Closure(string): int $lag seconds of lag per consumer group */
    private function broker(\Closure $lag): void
    {
        $this->app->bind(QueueWaits::class, function () use ($lag): QueueWaits {
            $rows = [];
            foreach (['laravel', 'emails'] as $group) {
                $rows[] = ['queue_name' => 'high', 'consumer_group' => $group, 'time_lag_seconds' => $lag($group)];
            }
            $handler = function ($request) use ($rows) {
                $path = $request->getUri()->getPath();
                $body = str_contains($path, '/depth') ? ['effectivePending' => 3] : $rows;

                return new \GuzzleHttp\Promise\FulfilledPromise(new \GuzzleHttp\Psr7\Response(200, ['Content-Type' => 'application/json'], json_encode($body)));
            };

            return new QueueWaits(new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create($handler),
            ]));
        });
    }
}
