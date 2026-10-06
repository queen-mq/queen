<?php

namespace Queen\Tests;

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
    // Helpers
    // ===========================

    /** @param list<mixed> $pops */
    private function broker(array $pops, array $acks = [], int $emptyPopsAfterScript = 0): void
    {
        $this->broker = new ConsumeBroker($pops, $acks, $emptyPopsAfterScript);
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
