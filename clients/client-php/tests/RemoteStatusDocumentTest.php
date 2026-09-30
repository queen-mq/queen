<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\RemoteStatusDocument;

/**
 * The chunked key/value format of a published supervisor status document.
 *
 * The Rust engine writes the same format, so the exact keys and value shapes
 * asserted here are a cross-engine contract, not an implementation detail.
 */
final class RemoteStatusDocumentTest extends TestCase
{
    private const WRITE_ID = '0123456789abcdef0123456789abcdef';

    public function testASmallDocumentIsOneHeadAndOneChunkWithTheSameTtl(): void
    {
        $document = ['schema' => 'queen.supervisor.status/v1', 'state' => 'running', 'pool_status' => []];

        $operations = RemoteStatusDocument::operations($document, 'queen-supervisor', 'orders', 600, self::WRITE_ID);

        $this->assertCount(2, $operations);
        $encoded = json_encode($document, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        $this->assertSame([
            'op' => 'put',
            'ns' => 'queen-supervisor',
            'key' => 'orders/head',
            'value' => [
                'format' => 'queen.supervisor.remote-status/v1',
                'write' => self::WRITE_ID,
                'chunks' => 1,
                'bytes' => strlen($encoded),
            ],
            'ttlSeconds' => 600,
        ], $operations[0]);
        $this->assertSame([
            'op' => 'put',
            'ns' => 'queen-supervisor',
            'key' => 'orders/chunk/0000',
            'value' => ['write' => self::WRITE_ID, 'index' => 0, 'data' => base64_encode($encoded)],
            'ttlSeconds' => 600,
        ], $operations[1]);
    }

    public function testADocumentAboveTheValueCeilingIsSplitAndReassembled(): void
    {
        $document = $this->largeDocument(150000);

        $operations = RemoteStatusDocument::operations($document, 'queen-supervisor', 'orders', 600, self::WRITE_ID);

        $this->assertCount(5, $operations);
        foreach ($operations as $operation) {
            $this->assertLessThan(65536, strlen(json_encode($operation['value'])));
        }
        $this->assertSame(['orders/chunk/0000', 'orders/chunk/0001', 'orders/chunk/0002', 'orders/chunk/0003'], array_column(array_slice($operations, 1), 'key'));
        $this->assertSame($document, RemoteStatusDocument::decode($this->rows($operations), 'orders'));
    }

    public function testChunksLeftBehindByAnEarlierLargerWriteAreIgnored(): void
    {
        $earlier = RemoteStatusDocument::operations($this->largeDocument(150000), 'ns', 'orders', 600, str_repeat('a', 32));
        $current = ['state' => 'paused', 'pool_status' => []];
        $latest = RemoteStatusDocument::operations($current, 'ns', 'orders', 600, self::WRITE_ID);

        $rows = array_values(array_merge(
            array_column($this->rows($earlier), null, 'key'),
            array_column($this->rows($latest), null, 'key'),
        ));

        $this->assertSame($current, RemoteStatusDocument::decode($rows, 'orders'));
    }

    public function testAnIncompleteOrMixedGenerationIsRejected(): void
    {
        $operations = RemoteStatusDocument::operations($this->largeDocument(100000), 'ns', 'orders', 600, self::WRITE_ID);
        $rows = $this->rows($operations);

        $missing = $rows;
        unset($missing[2]);
        $this->assertNull(RemoteStatusDocument::decode(array_values($missing), 'orders'));

        $mixed = $rows;
        $mixed[2]['value']['write'] = str_repeat('b', 32);
        $this->assertNull(RemoteStatusDocument::decode($mixed, 'orders'));

        $short = $rows;
        $short[1]['value']['data'] = base64_encode(str_repeat('x', 44999));
        $this->assertNull(RemoteStatusDocument::decode($short, 'orders'));

        $notBase64 = $rows;
        $notBase64[1]['value']['data'] = '%%%';
        $this->assertNull(RemoteStatusDocument::decode($notBase64, 'orders'));

        $this->assertNull(RemoteStatusDocument::decode(array_slice($rows, 1), 'orders'));
        $this->assertNull(RemoteStatusDocument::decode(['orders/head' => $rows[0]], 'orders'));
        $this->assertNull(RemoteStatusDocument::decode($rows, 'another-key'));
    }

    public function testAMalformedHeadIsRejected(): void
    {
        $rows = $this->rows(RemoteStatusDocument::operations(['state' => 'running'], 'ns', 'orders', 600, self::WRITE_ID));

        foreach ([
            ['format' => 'something-else'],
            ['write' => 'not-hex'],
            ['chunks' => 0],
            ['chunks' => RemoteStatusDocument::MAX_CHUNKS + 1],
            ['bytes' => RemoteStatusDocument::MAX_DOCUMENT_BYTES + 1],
            ['bytes' => '42'],
        ] as $override) {
            $malformed = $rows;
            $malformed[0]['value'] = array_replace($malformed[0]['value'], $override);
            $this->assertNull(RemoteStatusDocument::decode($malformed, 'orders'), json_encode($override));
        }
    }

    public function testADocumentAboveTheStatusFileCeilingIsRefused(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('1048576-byte ceiling');

        RemoteStatusDocument::operations($this->largeDocument(1048577), 'ns', 'orders', 600, self::WRITE_ID);
    }

    public function testTheLargestAllowedDocumentFitsTheChunkBudget(): void
    {
        $operations = RemoteStatusDocument::operations(
            $this->largeDocument(RemoteStatusDocument::MAX_DOCUMENT_BYTES - 64),
            'ns',
            'orders',
            600,
            self::WRITE_ID,
        );

        $this->assertLessThanOrEqual(RemoteStatusDocument::MAX_CHUNKS + 1, count($operations));
        $this->assertLessThanOrEqual(RemoteStatusDocument::PREFIX_LIMIT, 2 * count($operations));
    }

    public function testEveryInstanceIsDecodedFromItsOwnSlotInKeyOrder(): void
    {
        $php = ['instance_id' => str_repeat('b', 32), 'engine' => 'php'];
        $rust = ['instance_id' => '000000000000000018d9e392917a928f00000001', 'engine' => 'rust'];

        $rows = $this->slotRows([$php, $rust], 'orders');

        $this->assertSame([$rust, $php], RemoteStatusDocument::decodeAll($rows, 'orders'));
    }

    public function testTheSlotOfEarlierReleasesIsStillDecoded(): void
    {
        $legacy = ['instance_id' => str_repeat('c', 32), 'state' => 'running'];
        $current = ['instance_id' => str_repeat('d', 32), 'state' => 'running'];
        $rows = $this->sorted([
            ...$this->rows(RemoteStatusDocument::operations($legacy, 'ns', 'orders', 600, self::WRITE_ID)),
            ...$this->slotRows([$current], 'orders'),
        ]);

        $this->assertSame([$legacy, $current], RemoteStatusDocument::decodeAll($rows, 'orders'));
    }

    public function testADocumentFiledUnderAnotherInstanceIsIgnored(): void
    {
        $document = ['instance_id' => str_repeat('a', 32)];
        $rows = $this->rows(RemoteStatusDocument::operations(
            $document,
            'ns',
            RemoteStatusDocument::instanceKey('orders', str_repeat('b', 32)),
            600,
            self::WRITE_ID,
        ));

        $this->assertSame([], RemoteStatusDocument::decodeAll($rows, 'orders'));
    }

    public function testOneBrokenSlotDoesNotHideTheOthers(): void
    {
        $rows = $this->slotRows([['instance_id' => str_repeat('a', 32)], ['instance_id' => str_repeat('b', 32)]], 'orders');
        $rows[0]['value']['write'] = str_repeat('e', 32);

        $this->assertSame([['instance_id' => str_repeat('b', 32)]], RemoteStatusDocument::decodeAll($rows, 'orders'));
        $this->assertSame([], RemoteStatusDocument::decodeAll('not rows', 'orders'));
    }

    public function testANumericKeyIsNotMistakenForAnInstanceSlot(): void
    {
        $legacy = ['state' => 'running'];
        $current = ['instance_id' => str_repeat('d', 32)];
        $rows = $this->sorted([
            ...$this->rows(RemoteStatusDocument::operations($legacy, 'ns', '2026', 600, self::WRITE_ID)),
            ...$this->slotRows([$current], '2026'),
        ]);

        $this->assertSame([$legacy, $current], RemoteStatusDocument::decodeAll($rows, '2026'));
    }

    public function testOnlyDocumentKeysBelongToASlot(): void
    {
        $instance = str_repeat('a', 40);

        $this->assertSame('orders', RemoteStatusDocument::slot('orders/head', 'orders'));
        $this->assertSame('orders', RemoteStatusDocument::slot('orders/chunk/0000', 'orders'));
        $this->assertSame("orders/{$instance}", RemoteStatusDocument::slot("orders/{$instance}/head", 'orders'));
        $this->assertSame("orders/{$instance}", RemoteStatusDocument::slot("orders/{$instance}/chunk/0023", 'orders'));
        foreach ([
            'other/head',
            'orders',
            'orders/',
            'orders/chunk/1',
            "orders/{$instance}",
            "orders/{$instance}/notes",
            "orders/{$instance}/{$instance}/head",
            'orders/NOT-AN-INSTANCE-ID/head',
            'orders/0123abc/head',
        ] as $key) {
            $this->assertNull(RemoteStatusDocument::slot($key, 'orders'), $key);
        }
    }

    public function testOnlyEngineGeneratedInstanceIdsNameASlot(): void
    {
        $this->assertSame(
            'orders/0123456789abcdef0123456789abcdef',
            RemoteStatusDocument::instanceKey('orders', '0123456789abcdef0123456789abcdef'),
        );

        foreach ([null, 42, '', 'ABCDEF0123456789', '0123456789abcde', 'head', str_repeat('a', 16) . '/x', str_repeat('a', 129)] as $invalid) {
            try {
                RemoteStatusDocument::instanceKey('orders', $invalid);
                $this->fail('An invalid instance id was accepted: ' . json_encode($invalid));
            } catch (\InvalidArgumentException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    /**
     * Rows of every document published into its instance slot, in byte order.
     *
     * @param list<array<string, mixed>> $documents
     * @return list<array{key: string, value: mixed}>
     */
    private function slotRows(array $documents, string $key): array
    {
        $rows = [];
        foreach ($documents as $document) {
            $slot = RemoteStatusDocument::instanceKey($key, $document['instance_id']);
            array_push($rows, ...$this->rows(RemoteStatusDocument::operations($document, 'ns', $slot, 600, self::WRITE_ID)));
        }

        return $this->sorted($rows);
    }

    /**
     * @param list<array{key: string, value: mixed}> $rows
     * @return list<array{key: string, value: mixed}>
     */
    private function sorted(array $rows): array
    {
        usort($rows, fn (array $a, array $b): int => strcmp($a['key'], $b['key']));

        return $rows;
    }

    /** @return array<string, mixed> */
    private function largeDocument(int $bytes): array
    {
        $document = ['state' => 'running', 'padding' => ''];
        $overhead = strlen(json_encode($document, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
        $document['padding'] = str_repeat('p', $bytes - $overhead);

        return $document;
    }

    /**
     * The rows a getPrefix call would return after the operations applied.
     *
     * @param list<array<string, mixed>> $operations
     * @return list<array{key: string, value: mixed}>
     */
    private function rows(array $operations): array
    {
        return array_map(
            fn (array $operation): array => ['key' => $operation['key'], 'value' => $operation['value']],
            $operations,
        );
    }
}
