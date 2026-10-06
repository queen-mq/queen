<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\LazyLeaseRenewer;
use Queen\Laravel\Queue\LeaseRenewerFactory;
use Queen\Tests\Support\PlanHandler;

/**
 * The one place that reads the lease_renewal_* keys and builds a renewer, for
 * the queue connection's workers and for queen:consume alike.
 */
final class LeaseRenewerFactoryTest extends TestCase
{
    public function testTimingDefaultsTheIntervalToAThirdOfTheLease(): void
    {
        $this->assertSame(
            ['interval' => 30, 'timeout' => 5, 'killGrace' => 2, 'safetyMargin' => 1],
            LeaseRenewerFactory::timing([], 90),
        );
        $this->assertSame(1, LeaseRenewerFactory::timing(['lease_renewal_interval' => ''], 2)['interval']);
    }

    public function testTimingRefusesAnInvalidKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Queen Laravel lease_renewal_timeout must be an integer');

        LeaseRenewerFactory::timing(['lease_renewal_timeout' => 0], 90);
    }

    public function testMakeRefusesATimingThatDoesNotFitAndNamesTheLease(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('must be shorter than --lease');

        (new LeaseRenewerFactory())->make(['url' => 'http://queen.test:6632'], 10, LeaseRenewerFactory::timing([], 10), '--lease');
    }

    public function testEveryBackendCountsInTheRequestBudget(): void
    {
        $timing = LeaseRenewerFactory::timing(['lease_renewal_interval' => 13], 40);
        $factory = new LeaseRenewerFactory();

        // One backend: 13 + 5 + 5 + 1 + 2 + 1 = 27, inside 40.
        $this->assertInstanceOf(LazyLeaseRenewer::class, $factory->make(['url' => 'http://a:6632'], 40, $timing));

        // Three backends: a request budget is 15, and 13 + 15 + 15 is 43 already.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('timing is unsafe');
        $factory->make(['urls' => ['http://a:6632', 'http://b:6632', 'http://c:6632']], 40, $timing);
    }

    public function testMakeRefusesTheInternalTestHandler(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('test HTTP handler');

        (new LeaseRenewerFactory())->make(
            ['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create(new PlanHandler())],
            120,
            LeaseRenewerFactory::timing([], 120),
        );
    }

    public function testTheRenewerStartsNothingUntilALeaseIsTracked(): void
    {
        $renewer = (new LeaseRenewerFactory())->make(['url' => 'http://queen.test:6632'], 120, LeaseRenewerFactory::timing([], 120));

        $this->assertInstanceOf(LazyLeaseRenewer::class, $renewer);
        $this->assertNull((new \ReflectionProperty($renewer, 'delegate'))->getValue($renewer));
    }
}
