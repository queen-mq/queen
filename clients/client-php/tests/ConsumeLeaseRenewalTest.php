<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * renewLease(true, $intervalMillis) on the consume() loop.
 *
 * The handler runs synchronously on the only thread, so the loop can renew
 * only where it has control: before each message in each() mode, before the
 * handler in batch mode. The interval runs from the pop answer, when the lease
 * began, so a batch that waited behind another poller's handler is renewed
 * before its own handler starts.
 */
final class ConsumeLeaseRenewalTest extends TestCase
{
    private const INTERVAL_MILLIS = 100;
    private const SLOW_HANDLER_MICROS = 250_000;

    public function testBatchThatWaitedBehindAnotherPollerIsRenewedBeforeItsHandler(): void
    {
        $handler = new PlanHandler([self::popAnswer('lease-a'), self::popAnswer('lease-b')]);
        $handled = [];

        $this->queen($handler)->queue('orders')->group('workers')
            ->concurrency(2)->limit(2)->autoAck(false)
            ->renewLease(true, self::INTERVAL_MILLIS)
            ->consume(function (array $messages) use (&$handled): void {
                $handled[] = $messages[0]['leaseId'];
                if (count($handled) === 1) {
                    usleep(self::SLOW_HANDLER_MICROS);
                }
            })
            ->execute();

        $this->assertSame(['lease-a', 'lease-b'], $handled);
        $this->assertSame(['/api/v1/lease/lease-b/extend'], self::extends($handler));
    }

    public function testEachModeRenewsBetweenMessagesOnceTheIntervalPassed(): void
    {
        $handler = new PlanHandler([self::popAnswer('lease-1', messages: 2)]);
        $handled = 0;

        $this->queen($handler)->queue('orders')->group('workers')
            ->each()->limit(2)->autoAck(false)
            ->renewLease(true, self::INTERVAL_MILLIS)
            ->consume(function () use (&$handled): void {
                if (++$handled === 1) {
                    usleep(self::SLOW_HANDLER_MICROS);
                }
            })
            ->execute();

        $this->assertSame(2, $handled);
        $this->assertContains('/api/v1/lease/lease-1/extend', self::extends($handler));
    }

    /**
     * The documented limit: the loop has no control while a handler runs, so a
     * batch handled as soon as it arrives is never renewed by the loop.
     */
    public function testBatchHandledAtOnceIsNotRenewedWhileItsHandlerRuns(): void
    {
        $handler = new PlanHandler([self::popAnswer('lease-1')]);

        $this->queen($handler)->queue('orders')->group('workers')
            ->limit(1)->autoAck(false)
            ->renewLease(true, self::INTERVAL_MILLIS)
            ->consume(function (): void {
                usleep(self::SLOW_HANDLER_MICROS);
            })
            ->execute();

        $this->assertSame([], self::extends($handler));
    }

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);
    }

    private static function popAnswer(string $leaseId, int $messages = 1): array
    {
        return ['status' => 200, 'json' => ['messages' => array_map(fn(int $i): array => [
            'transactionId' => "{$leaseId}-tx-{$i}",
            'partitionId' => 'p1',
            'queue' => 'orders',
            'partition' => 'Default',
            'data' => ['n' => $i],
            'leaseId' => $leaseId,
        ], range(1, $messages))]];
    }

    /** @return list<string> the path of every lease renewal, in order */
    private static function extends(PlanHandler $handler): array
    {
        return array_values(array_filter(
            array_map(fn(RequestInterface $r): string => $r->getUri()->getPath(), $handler->requests),
            fn(string $path): bool => str_ends_with($path, '/extend'),
        ));
    }
}
