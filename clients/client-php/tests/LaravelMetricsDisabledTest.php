<?php

namespace Queen\Tests;

use InvalidArgumentException;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;

final class LaravelMetricsDisabledTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    public function testTheRouteIsAbsentByDefault(): void
    {
        $this->get('/queen/metrics')->assertNotFound();
        $this->assertFalse($this->app['router']->has('queen.metrics'));
    }

    public function testAShortTokenIsRefusedAtBoot(): void
    {
        $this->app['config']->set('queen.metrics', ['enabled' => true, 'token' => 'short']);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('queen.metrics.token');
        (new QueenServiceProvider($this->app))->boot();
    }
}
