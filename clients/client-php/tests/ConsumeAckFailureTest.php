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

    /**
     * The automatic renewal of renewLease(): one that renewed nothing leaves
     * the message to another consumer once its lease expires, while the
     * handler still runs. The log says so; the lease id is sent encoded, and
     * the request waits no longer than the renewal interval.
     */
    public function testARenewalThatRenewedNothingIsReported(): void
    {
        $message = fn (string $id): array => ['transactionId' => $id, 'partitionId' => 'p-1', 'leaseId' => 'lease/1', 'data' => []];
        $handler = new PlanHandler([
            ['status' => 200, 'json' => ['messages' => [$message('t-1'), $message('t-2')]]],
            ['status' => 200, 'json' => ['success' => true]],
            ['status' => 200, 'json' => ['success' => true, 'renewed' => 0]],
        ], ['status' => 200, 'json' => ['success' => true]]);
        $queen = new Queen([
            'url' => 'http://queen.test:6632',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($handler),
        ]);

        // The renewal is due before the second message.
        $logged = self::errorLogOf(fn () => $queen->queue('orders')->group('workers')->limit(2)
            ->renewLease(true, 1)->each()
            ->consume(function (): void {
                usleep(5_000);
            })->execute());

        $extends = array_keys(array_filter(
            $handler->requests,
            fn ($request): bool => str_ends_with($request->getUri()->getPath(), '/extend'),
        ));
        $this->assertCount(1, $extends);
        $this->assertSame('/api/v1/lease/lease%2F1/extend', $handler->requests[$extends[0]]->getUri()->getPath());
        $this->assertLessThanOrEqual(1, $handler->options[$extends[0]]['timeout'] ?? null);
        $this->assertStringContainsString('could not renew lease lease/1: the broker renewed no lease', $logged);
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
