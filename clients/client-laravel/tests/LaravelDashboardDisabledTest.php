<?php

namespace Queen\Tests;

use Orchestra\Testbench\TestCase;
use Queen\Laravel\QueenServiceProvider;

final class LaravelDashboardDisabledTest extends TestCase
{
    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queen.dashboard.enabled', false);
    }

    public function testDashboardRoutesAreAbsentByDefault(): void
    {
        $this->get('/queen')->assertNotFound();
        $this->get('/queen/api/status')->assertNotFound();
        $this->post('/queen/control/pause')->assertNotFound();
    }
}
