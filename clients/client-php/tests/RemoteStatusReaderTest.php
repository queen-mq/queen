<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\RemoteStatusReader;
use Queen\Laravel\Supervisor\RemoteStatusDocument;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class RemoteStatusReaderTest extends TestCase
{
    private const A = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

    private const B = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';

    public function testOnePageReadsEveryPublishedInstance(): void
    {
        $handler = new PlanHandler([$this->page([...$this->slot(self::A), ...$this->slot(self::B)], false)]);

        $documents = $this->reader($handler)->read();

        $this->assertSame([self::A, self::B], array_column($documents, 'instance_id'));
        $this->assertSame([[
            'op' => 'getPrefix',
            'ns' => 'queen-supervisor',
            'prefix' => 'orders/',
            'limit' => RemoteStatusDocument::PREFIX_LIMIT,
        ]], $this->operations($handler, 0));
    }

    public function testATruncatedPageHoldsBackTheSlotItCutAndReadsItWholeOnTheNextPage(): void
    {
        $a = $this->slot(self::A);
        $b = $this->slot(self::B);
        $handler = new PlanHandler([
            // Cut after B's chunk: B's head is on the next page.
            $this->page([...$a, $b[0]], true),
            $this->page($b, false),
        ]);

        $documents = $this->reader($handler)->read();

        $this->assertSame([self::A, self::B], array_column($documents, 'instance_id'));
        $this->assertSame(2, $handler->count());
        $this->assertSame(end($a)['key'], $this->operations($handler, 1)[0]['after']);
    }

    public function testAPageHoldingPartOfOneSlotOnlyIsKeptAndPagingContinues(): void
    {
        $a = $this->slot(self::A);
        $handler = new PlanHandler([
            $this->page([$a[0]], true),
            $this->page([$a[1]], false),
        ]);

        $documents = $this->reader($handler)->read();

        $this->assertSame([self::A], array_column($documents, 'instance_id'));
        $this->assertSame($a[0]['key'], $this->operations($handler, 1)[0]['after']);
    }

    public function testAFailureAfterTheFirstPageKeepsTheInstancesAlreadyRead(): void
    {
        $b = $this->slot(self::B);
        $handler = new PlanHandler([
            $this->page([...$this->slot(self::A), $b[0]], true),
            ['status' => 503, 'json' => ['error' => 'unavailable']],
        ]);

        $this->assertSame([self::A], array_column($this->reader($handler)->read(), 'instance_id'));
    }

    public function testAnUnreachableBrokerOrAMalformedPageReadsNothing(): void
    {
        $unreachable = new PlanHandler([['status' => 503, 'json' => ['error' => 'unavailable']]]);
        $this->assertSame([], $this->reader($unreachable)->read());

        $unknownTruncation = new PlanHandler([['status' => 200, 'json' => ['results' => [['rows' => $this->slot(self::A)]]]]]);
        $this->assertSame([], $this->reader($unknownTruncation)->read());

        $keyless = new PlanHandler([$this->page([['value' => []]], false)]);
        $this->assertSame([], $this->reader($keyless)->read());
    }

    public function testPagingStopsAtItsBound(): void
    {
        $pages = [];
        for ($index = 1; $index <= 18; ++$index) {
            $pages[] = $this->page($this->slot(sprintf('%032x', $index)), true);
        }
        $handler = new PlanHandler($pages);

        $this->assertCount(16, $this->reader($handler)->read());
        $this->assertSame(16, $handler->count());
    }

    public function testAPageThatMakesNoProgressEndsPaging(): void
    {
        $handler = new PlanHandler([], $this->page($this->slot(self::A), true));

        $this->assertSame([self::A], array_column($this->reader($handler)->read(), 'instance_id'));
        $this->assertSame(2, $handler->count());
    }

    private function reader(PlanHandler $handler): RemoteStatusReader
    {
        return new RemoteStatusReader(
            new Queen([
                'url' => 'http://queen.test:6632',
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
                'handler' => HandlerStack::create($handler),
            ]),
            'queen-supervisor',
            'orders',
        );
    }

    /**
     * The rows one instance's publish leaves, in byte order: its chunk, then its head.
     *
     * @return list<array{key: string, value: mixed}>
     */
    private function slot(string $instanceId): array
    {
        $operations = RemoteStatusDocument::operations(
            ['instance_id' => $instanceId, 'state' => 'running'],
            'queen-supervisor',
            RemoteStatusDocument::instanceKey('orders', $instanceId),
            600,
            str_repeat('d', 32),
        );
        $rows = array_map(fn (array $op): array => ['key' => $op['key'], 'value' => $op['value']], $operations);
        usort($rows, fn (array $a, array $b): int => strcmp($a['key'], $b['key']));

        return $rows;
    }

    /** @param list<array<string, mixed>> $rows */
    private function page(array $rows, bool $truncated): array
    {
        return ['status' => 200, 'json' => ['results' => [[
            'rows' => $rows,
            'truncated' => $truncated,
            'nextAfter' => $truncated ? end($rows)['key'] : null,
        ]]]];
    }

    /** @return list<array<string, mixed>> */
    private function operations(PlanHandler $handler, int $request): array
    {
        return json_decode((string) $handler->requests[$request]->getBody(), true)['operations'];
    }
}
