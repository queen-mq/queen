<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

/**
 * consume() with autoAck acknowledges each message (or batch) after its
 * handler. Queen::ack() returns a failure instead of throwing it, and the
 * loop must not drop that result: the message comes back when its lease
 * expires, and the log is the only place that says why.
 */
final class ConsumeAckFailureTest extends TestCase
{
    /** What $run wrote with error_log(). */
    private static function errorLogOf(callable $run): string
    {
        $log = (string) tempnam(sys_get_temp_dir(), 'queen-ack-log');
        $previous = ini_set('error_log', $log);
        try {
            $run();
        } finally {
            ini_set('error_log', (string) $previous);
        }
        $logged = (string) file_get_contents($log);
        @unlink($log);

        return $logged;
    }

    #[\PHPUnit\Framework\Attributes\TestWith([false, false])]
    #[\PHPUnit\Framework\Attributes\TestWith([true, false])]
    #[\PHPUnit\Framework\Attributes\TestWith([false, true])]
    #[\PHPUnit\Framework\Attributes\TestWith([true, true])]
    public function testAFailedAckIsReported(bool $each, bool $handlerFails): void
    {
        $handler = new PlanHandler(
            [['status' => 200, 'json' => ['messages' => [[
                'transactionId' => 't-1',
                'partitionId' => 'p-1',
                'leaseId' => 'l-1',
                'data' => [],
            ]]]]],
            ['status' => 500, 'json' => ['error' => 'ack storage exploded']],
        );
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        $builder = $queen->queue('orders')->group('workers')->limit(1);
        if ($each) {
            $builder->each();
        }
        $logged = self::errorLogOf(fn () => $builder->consume(function () use ($handlerFails): void {
            if ($handlerFails) {
                throw new \RuntimeException('handler failed');
            }
        })->execute());

        $this->assertSame(2, $handler->count(), 'one pop and one ACK');
        $this->assertStringContainsString('could not acknowledge 1 message', $logged);
        $this->assertStringContainsString($handlerFails ? 'as failed' : 'as completed', $logged);
        $this->assertStringContainsString('ack storage exploded', $logged);
    }

    public function testASuccessfulAckLogsNothing(): void
    {
        $handler = new PlanHandler(
            [['status' => 200, 'json' => ['messages' => [[
                'transactionId' => 't-1',
                'partitionId' => 'p-1',
                'leaseId' => 'l-1',
                'data' => [],
            ]]]]],
            ['status' => 200, 'json' => ['success' => true]],
        );
        $queen = new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]);

        $logged = self::errorLogOf(fn () => $queen->queue('orders')->group('workers')->limit(1)
            ->consume(function (): void {
            })->execute());

        $this->assertSame('', $logged);
        $this->assertSame(2, $handler->count(), 'one pop and one ACK');
    }
}
