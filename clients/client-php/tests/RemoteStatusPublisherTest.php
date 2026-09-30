<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Laravel\Supervisor\RemoteStatusPublisher;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class RemoteStatusPublisherTest extends TestCase
{
    private const INSTANCE = '0123456789abcdef0123456789abcdef';

    private float $now = 1000.0;

    /** @var list<array{0: string, 1: string}> */
    private array $output = [];

    public function testItPublishesTheDocumentAsOneBatchWithoutTheLegacyPoolMap(): void
    {
        $handler = $this->applyingHandler();

        $this->publisher($handler)->publish($this->document('running'));

        $this->assertSame(1, $handler->count());
        $request = $handler->requests[0];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/kv', $request->getUri()->getPath());
        $operations = json_decode((string) $request->getBody(), true)['operations'];
        // Each instance owns a slot, so pods sharing the key never overwrite each other.
        $this->assertSame(
            ['orders/' . self::INSTANCE . '/head', 'orders/' . self::INSTANCE . '/chunk/0000'],
            array_column($operations, 'key'),
        );
        $this->assertSame([900, 900], array_column($operations, 'ttlSeconds'));
        $published = RemoteStatusDocument::decode(
            array_map(fn (array $op): array => ['key' => $op['key'], 'value' => $op['value']], $operations),
            'orders/' . self::INSTANCE,
        );
        $this->assertArrayNotHasKey('pools', $published);
        $this->assertSame([['supervisor' => 'default', 'queue' => 'high']], $published['pool_status']);
        $this->assertSame([], $this->output);
    }

    public function testItPublishesAtMostOncePerIntervalUnlessTheStateChanges(): void
    {
        $handler = $this->applyingHandler();
        $publisher = $this->publisher($handler);

        $publisher->publish($this->document('running'));
        $this->now += 4.9;
        $publisher->publish($this->document('running'));
        $this->assertSame(1, $handler->count());

        $this->now += 0.1;
        $publisher->publish($this->document('running'));
        $this->assertSame(2, $handler->count());

        $this->now += 1;
        $publisher->publish($this->document('paused'));
        $this->assertSame(3, $handler->count());
    }

    public function testAFailureIsReportedOncePerStreakAndNeverThrown(): void
    {
        $handler = new PlanHandler([
            ['status' => 503, 'json' => ['error' => 'unavailable']],
            ['status' => 200, 'json' => ['results' => [['applied' => false, 'reason' => 'limit'], ['applied' => true]]]],
        ], ['status' => 200, 'json' => ['results' => [['applied' => true], ['applied' => true]]]]);
        $publisher = $this->publisher($handler);

        $publisher->publish($this->document('running'));
        $this->now += 5;
        $publisher->publish($this->document('running'));
        $this->assertCount(1, $this->output);
        $this->assertSame('err', $this->output[0][1]);
        $this->assertStringContainsString('remote status publish failed', $this->output[0][0]);

        $this->now += 5;
        $publisher->publish($this->document('running'));
        $this->assertCount(2, $this->output);
        $this->assertStringContainsString('recovered', $this->output[1][0]);
    }

    public function testAFailedPublishWaitsForTheNextIntervalInsteadOfRetryingEveryLoop(): void
    {
        $handler = new PlanHandler([], ['status' => 503, 'json' => ['error' => 'unavailable']]);
        $publisher = $this->publisher($handler);

        $publisher->publish($this->document('running'));
        $this->now += 1;
        $publisher->publish($this->document('running'));

        $this->assertSame(1, $handler->count());
    }

    public function testAnUnexpectedBatchResponseIsAFailure(): void
    {
        $handler = new PlanHandler([], ['status' => 200, 'json' => [
            'ok' => false,
            'reason' => 'kv_precondition',
        ]]);

        $this->publisher($handler)->publish($this->document('running'));

        $this->assertCount(1, $this->output);
        $this->assertStringContainsString('kv_precondition', $this->output[0][0]);
    }

    public function testADocumentWithoutAValidInstanceIdIsAFailureAndIsNeverSent(): void
    {
        $handler = $this->applyingHandler();
        $document = $this->document('running');
        $document['instance_id'] = '../another-key';

        $this->publisher($handler)->publish($document);

        $this->assertSame(0, $handler->count());
        $this->assertCount(1, $this->output);
        $this->assertStringContainsString('no valid instance_id', $this->output[0][0]);
    }

    private function publisher(PlanHandler $handler): RemoteStatusPublisher
    {
        return new RemoteStatusPublisher(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create($handler),
            ]),
            'queen-supervisor',
            'orders',
            5,
            900,
            function (string $buffer, string $type): void {
                $this->output[] = [$buffer, $type];
            },
            fn (): float => $this->now,
        );
    }

    private function applyingHandler(): PlanHandler
    {
        return new PlanHandler([], ['status' => 200, 'json' => ['results' => [
            ['applied' => true],
            ['applied' => true],
        ]]]);
    }

    /** @return array<string, mixed> */
    private function document(string $state): array
    {
        return [
            'schema' => 'queen.supervisor.status/v1',
            'state' => $state,
            'instance_id' => self::INSTANCE,
            'pools' => ['default' => ['high' => ['processes' => 1]]],
            'pool_status' => [['supervisor' => 'default', 'queue' => 'high']],
        ];
    }
}
