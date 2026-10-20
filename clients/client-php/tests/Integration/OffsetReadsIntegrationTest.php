<?php

namespace Queen\Tests\Integration;

/**
 * Reads by offset (queue()->fetch()) and partition discovery
 * (queue()->changedSince()) against a live broker.
 *
 * The queues get a run-unique name and are deleted in tearDown: nothing here
 * writes KV or timers, so the fixed-name rule of IntegrationTestCase (which is
 * about those) does not apply.
 */
class OffsetReadsIntegrationTest extends IntegrationTestCase
{
    private string $fetchQueue;
    private string $changedQueue;

    protected function setUp(): void
    {
        parent::setUp();
        $run = bin2hex(random_bytes(4));
        $this->fetchQueue = "php-reads-fetch-{$run}";
        $this->changedQueue = "php-reads-changed-{$run}";
    }

    protected function tearDown(): void
    {
        foreach ([$this->fetchQueue ?? null, $this->changedQueue ?? null] as $queue) {
            if ($queue === null) {
                continue;
            }
            try {
                $this->queen->queue($queue)->delete()->execute();
            } catch (\Throwable) {
            }
        }
        parent::tearDown();
    }

    private function pushAll(string $queue, array $items): void
    {
        foreach (array_chunk($items, 500) as $chunk) {
            $results = $this->queen->queue($queue)->push($chunk)->execute();
            foreach ($results as $r) {
                $this->assertSame('queued', $r['status'] ?? null);
            }
        }
    }

    public function testFetchReadsEveryRecordByOffset(): void
    {
        // 1,200 partitions: more than one fetch call (1000 entries each).
        $partitions = array_map(fn(int $i) => "entity-{$i}", range(0, 1199));
        $perPartition = 3;
        for ($n = 0; $n < $perPartition; $n++) {
            $this->pushAll($this->fetchQueue, array_map(
                fn(string $p) => ['partition' => $p, 'transactionId' => "{$p}-{$n}", 'data' => ['p' => $p, 'n' => $n]],
                $partitions,
            ));
        }

        $read = $this->queen->queue($this->fetchQueue)->fetch($partitions);

        $this->assertCount(count($partitions), $read);
        foreach ($partitions as $p) {
            $part = $read[$p];
            // assertEquals: the broker answers a payload object with its keys
            // sorted, and assertSame would compare the key order.
            $this->assertEquals(
                array_map(fn(int $n) => [$n, ['p' => $p, 'n' => $n], "{$p}-{$n}"], range(0, $perPartition - 1)),
                array_map(fn(array $r) => [$r['offset'], $r['payload'], $r['transactionId']], $part['records']),
                $p,
            );
            $this->assertSame([$perPartition, 0, $perPartition], [$part['highWatermark'], $part['logStartOffset'], $part['nextOffset']]);
        }

        // A later read from nextOffset answers only what came after.
        $this->queen->queue($this->fetchQueue)->partition('entity-7')->push([['data' => ['late' => true]]])->execute();
        $later = $this->queen->queue($this->fetchQueue)->fetch(['entity-7', 'entity-8'], [
            'entity-7' => $read['entity-7']['nextOffset'],
            'entity-8' => $read['entity-8']['nextOffset'],
        ]);
        $this->assertSame([[$perPartition, ['late' => true]]], array_map(fn(array $r) => [$r['offset'], $r['payload']], $later['entity-7']['records']));
        $this->assertSame([], $later['entity-8']['records']);

        // The reads took no lease and moved no cursor: a pop still gets the messages.
        $popped = $this->queen->queue($this->fetchQueue)->partition('entity-9')->batch(10)->wait(false)->pop();
        $this->assertCount($perPartition, $popped);

        $missing = $this->queen->queue($this->fetchQueue . '-does-not-exist')->fetch(['x']);
        $this->assertSame([], $missing['x']['records']);
    }

    public function testChangedSinceListsTheWrittenPartitions(): void
    {
        $before = new \DateTimeImmutable('-1 minute');
        $names = array_map(fn(int $i) => "c-{$i}", range(0, 1099));
        $this->pushAll($this->changedQueue, array_map(fn(string $p) => ['partition' => $p, 'data' => ['p' => $p]], $names));

        // 1,100 partitions: two pages.
        $first = $this->queen->queue($this->changedQueue)->changedSince($before);
        $listed = array_column($first['partitions'], 'name');
        sort($listed);
        $want = $names;
        sort($want);
        $this->assertSame($want, $listed);
        $this->assertIsString($first['safeTime']);
        $one = array_values(array_filter($first['partitions'], fn(array $p) => $p['name'] === 'c-5'))[0];
        $this->assertSame([0, 0], [$one['lastOffset'], $one['logStart']]);
        $this->assertIsString($one['id']);

        // From the safeTime of that pass, a write to one partition lists it, and
        // nothing written before that safeTime comes back.
        $this->queen->queue($this->changedQueue)->partition('c-42')->push([['data' => ['again' => true]]])->execute();
        $next = $this->queen->queue($this->changedQueue)->changedSince($first['safeTime']);
        $this->assertNotEmpty(array_filter($next['partitions'], fn(array $p) => $p['name'] === 'c-42' && $p['lastOffset'] === 1));
        foreach ($next['partitions'] as $p) {
            $this->assertGreaterThanOrEqual(0, strcmp($p['lastWriteAt'], $first['safeTime']), $p['name']);
        }

        $this->assertCount(count($names), $this->queen->queue($this->changedQueue)->changedSince()['partitions']);
        $this->assertSame([], $this->queen->queue($this->changedQueue . '-does-not-exist')->changedSince($before)['partitions']);
    }
}
