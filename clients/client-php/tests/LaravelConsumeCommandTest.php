<?php

namespace Queen\Tests;

use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Artisan;
use Orchestra\Testbench\TestCase;
use Queen\Laravel\Commands\ConsumeCommand;
use Queen\Laravel\QueenServiceProvider;
use Queen\Queen;
use Queen\Tests\Support\ConsumeBroker;
use Queen\Tests\Support\ConsumeHandler;

/**
 * php artisan queen:consume against a scripted broker: what it pops, what it
 * hands to the handler, what it acks, and what it prints.
 */
final class LaravelConsumeCommandTest extends TestCase
{
    private ConsumeBroker $broker;

    private ConsumeHandler $handler;

    protected function getPackageProviders($app): array
    {
        return [QueenServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queen.retry_after', 90);
        $app['config']->set('queen.lease_renewal', false);
    }

    public function testAMessageReachesTheHandler(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]]);

        [$exit] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--limit' => 1]);

        $this->assertSame(0, $exit);
        $this->assertSame('tx-1', $this->handler->received[0]['transactionId']);
    }

    // ===========================
    // Help text
    // ===========================

    public function testBatchHelpSaysWhatTheOptionDoes(): void
    {
        $help = (new ConsumeCommand())->getDefinition()->getOption('batch')->getDescription();

        $this->assertStringNotContainsString('broker sizes it', $help);
        $this->assertStringContainsString('default 1', $help);
        $this->assertStringContainsString('list', $help);
    }

    public function testConflationHelpDoesNotSayItRefusesAutoAck(): void
    {
        $help = (new ConsumeCommand())->getDefinition()->getOption('conflation')->getDescription();

        $this->assertStringNotContainsString('auto-ack', $help);
        $this->assertStringContainsString('--group', $help);
    }

    // ===========================
    // --limit
    // ===========================

    public function testLimitCountsTheMessagesWhoseHandlerThrew(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')], [ConsumeBroker::message('tx-2')]]);

        [$exit] = $this->consume(['--group' => 'ledger', '--limit' => 2], function (): void {
            throw new \RuntimeException('handler failed');
        });

        $this->assertSame(0, $exit);
        $this->assertCount(2, $this->handler->received);
        $this->assertCount(2, $this->broker->pops());
    }

    public function testEachPopAsksForNoMoreThanTheLimitLeaves(): void
    {
        $this->broker([
            [ConsumeBroker::message('tx-1'), ConsumeBroker::message('tx-2')],
            [ConsumeBroker::message('tx-3')],
        ]);

        [$exit] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--batch' => 2, '--limit' => 3]);

        $this->assertSame(0, $exit);
        $this->assertSame(['2', '1'], array_column($this->broker->pops(), 'batch'));
        $this->assertCount(2, $this->handler->received[0]);
        $this->assertCount(1, $this->handler->received[1]);
    }

    // ===========================
    // Ack and nack
    // ===========================

    public function testAHandlerThatThrowsIsNackedWithoutAutoAck(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]]);

        [$exit] = $this->consume(['--group' => 'ledger', '--limit' => 1], function (): void {
            throw new \RuntimeException('handler failed');
        });

        $this->assertSame(0, $exit);
        $this->assertCount(1, $this->broker->acks);
        $this->assertSame('failed', $this->broker->acks[0]['status']);
        $this->assertSame('handler failed', $this->broker->acks[0]['error']);
        $this->assertSame('ledger', $this->broker->acks[0]['consumerGroup']);
        $this->assertSame('lease-1', $this->broker->acks[0]['leaseId']);
    }

    public function testABatchWhoseHandlerThrowsIsNackedInOneRequest(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1'), ConsumeBroker::message('tx-2')]]);

        $this->consume(['--group' => 'ledger', '--batch' => 2, '--limit' => 2], function (): void {
            throw new \RuntimeException('batch failed');
        });

        $this->assertCount(1, $this->broker->acks);
        $this->assertSame(['failed', 'failed'], array_column($this->broker->acks[0]['acknowledgments'], 'status'));
        $this->assertSame(['batch failed', 'batch failed'], array_column($this->broker->acks[0]['acknowledgments'], 'error'));
    }

    public function testWithoutAutoAckAHandlerThatReturnsLeavesTheAckToIt(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]]);

        $this->consume(['--group' => 'ledger', '--limit' => 1]);

        $this->assertSame([], $this->broker->acks);
    }

    public function testAutoAckAcksAHandlerThatReturns(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]]);

        $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--limit' => 1]);

        $this->assertCount(1, $this->broker->acks);
        $this->assertSame('completed', $this->broker->acks[0]['status']);
    }

    public function testARefusedAckPrintsOneWarningWithTheError(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]], [ConsumeBroker::refused(1, 'invalid or expired lease')]);

        [, $output] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--limit' => 1]);

        $this->assertSame(1, substr_count($output, 'Ack refused'));
        $this->assertStringContainsString('Ack refused for 1 of 1 message: invalid or expired lease', $output);
    }

    public function testAnAckCallThatFailsPrintsTheCountItCovered(): void
    {
        $this->broker(
            [[ConsumeBroker::message('tx-1'), ConsumeBroker::message('tx-2')]],
            [['error' => 'consumer group not found']],
        );

        [, $output] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--batch' => 2, '--limit' => 2]);

        $this->assertStringContainsString('Ack failed for 2 messages: consumer group not found', $output);
    }

    public function testANackRefusedForItsOwnReasonPrintsAWarning(): void
    {
        $this->broker([[ConsumeBroker::message('tx-1')]], [ConsumeBroker::refused(1, 'queue is paused')]);

        [, $output] = $this->consume(['--group' => 'ledger', '--limit' => 1], function (): void {
            throw new \RuntimeException('handler failed');
        });

        $this->assertStringContainsString('Nack refused for 1 of 1 message: queue is paused', $output);
    }

    public function testANackAfterAThrowIsSilentAboutWhatTheHandlerSettledItself(): void
    {
        $this->broker(
            [[ConsumeBroker::message('tx-1')], [ConsumeBroker::message('tx-2')]],
            [
                ConsumeBroker::refused(1, 'transaction is unresolvable, already committed, or acknowledgment is stale'),
                ConsumeBroker::refused(1, 'invalid or expired lease'),
            ],
        );

        [, $output] = $this->consume(['--group' => 'ledger', '--limit' => 2], function (): void {
            throw new \RuntimeException('acked, then failed');
        });

        $this->assertCount(2, $this->broker->acks);
        $this->assertStringNotContainsString('refused', $output);
    }

    // ===========================
    // --idle-timeout
    // ===========================

    public function testIdleTimeoutStopsTheCommandWithExitZero(): void
    {
        $this->broker([], [], emptyPopsAfterScript: 1000, emptyPopMicros: 5_000);

        $started = hrtime(true);
        [$exit, $output] = $this->consume(['--group' => 'ledger', '--idle-timeout' => 150]);
        $elapsedMillis = intdiv(hrtime(true) - $started, 1_000_000);

        $this->assertSame(0, $exit);
        $this->assertStringContainsString('No message for 150 ms (--idle-timeout): stopping.', $output);
        $this->assertGreaterThanOrEqual(150, $elapsedMillis);
        $this->assertLessThan(5_000, $elapsedMillis);
    }

    public function testAPopNeverWaitsPastTheIdleDeadline(): void
    {
        $this->broker([], [], emptyPopsAfterScript: 1000, emptyPopMicros: 5_000);

        $this->consume(['--group' => 'ledger', '--idle-timeout' => 150, '--timeout' => 30_000]);

        foreach ($this->broker->pops() as $pop) {
            $this->assertLessThanOrEqual(150, (int) $pop['timeout']);
        }
    }

    public function testIdleTimeoutCountsFromTheLastMessage(): void
    {
        $this->broker([
            function (): array {
                usleep(120_000);

                return [ConsumeBroker::message('tx-1')];
            },
        ], [], emptyPopsAfterScript: 1000, emptyPopMicros: 5_000);

        $started = hrtime(true);
        [$exit] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--idle-timeout' => 200]);
        $elapsedMillis = intdiv(hrtime(true) - $started, 1_000_000);

        $this->assertSame(0, $exit);
        $this->assertCount(1, $this->handler->received);
        $this->assertGreaterThanOrEqual(310, $elapsedMillis);
    }

    // ===========================
    // An unreachable broker
    // ===========================

    public function testAnUnreachableBrokerIsReportedOnceAndSoIsItsReturn(): void
    {
        $this->broker(['refused', 'refused', [ConsumeBroker::message('tx-1')]]);

        [$exit, $output] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--limit' => 1]);

        $this->assertSame(0, $exit);
        $this->assertSame(1, substr_count($output, 'unreachable'));
        $this->assertStringContainsString('Queen broker unreachable: cURL error 7', $output);
        $this->assertStringContainsString('Queen broker reachable again', $output);
    }

    public function testTheUnreachableWarningRepeatsAtMostEveryThirtySeconds(): void
    {
        Carbon::setTestNow('2026-10-06 12:00:00');
        $after = static function (int $seconds): \Closure {
            return static function () use ($seconds): string {
                Carbon::setTestNow(Carbon::parse('2026-10-06 12:00:00')->addSeconds($seconds));

                return 'refused';
            };
        };

        try {
            $this->broker([$after(0), $after(10), $after(29), $after(31), $after(40), [ConsumeBroker::message('tx-1')]]);

            [, $output] = $this->consume(['--group' => 'ledger', '--auto-ack' => true, '--limit' => 1]);
        } finally {
            Carbon::setTestNow();
        }

        $this->assertSame(2, substr_count($output, 'unreachable'));
        $this->assertStringContainsString('Queen broker still unreachable after 31 s: cURL error 7', $output);
        $this->assertStringContainsString('Queen broker reachable again after 40 s.', $output);
    }

    // ===========================
    // Helpers
    // ===========================

    /** @param list<mixed> $pops */
    private function broker(array $pops, array $acks = [], int $emptyPopsAfterScript = 0, int $emptyPopMicros = 0): void
    {
        $this->broker = new ConsumeBroker($pops, $acks, $emptyPopsAfterScript, $emptyPopMicros);
        $this->app->instance(Queen::class, $this->broker->queen());
    }

    /** @return array{0: int, 1: string} The exit code and the output. */
    private function consume(array $options, ?\Closure $handle = null): array
    {
        $this->handler = new ConsumeHandler($handle);
        $this->app->instance(ConsumeHandler::class, $this->handler);

        $exit = Artisan::call('queen:consume', array_replace([
            'queue' => 'payments',
            'handler' => ConsumeHandler::class,
        ], $options));

        return [$exit, Artisan::output()];
    }
}
