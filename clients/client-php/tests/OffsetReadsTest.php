<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Tests\Support\ReadsBroker;

/**
 * queue()->fetch() and queue()->changedSince() against Support\ReadsBroker, a
 * Guzzle handler that answers both routes the way the broker does (see its
 * header). The bodies the client sends are asserted as they reached the
 * handler.
 */
class OffsetReadsTest extends TestCase
{
    private const MIB = 1048576;

    private function queen(ReadsBroker $broker): Queen
    {
        return new Queen([
            'url' => 'http://queen.test',
            'retryAttempts' => 1,
            'handler' => HandlerStack::create($broker),
        ]);
    }

    private static function payloads(int $n): array
    {
        return array_map(fn(int $i) => ['n' => $i], range(0, $n - 1));
    }

    private static function offsets(array $part): array
    {
        return array_column($part['records'], 'offset');
    }

    public function testFetchReadsEveryRecordFromOffsetZeroInOneCallWhenTheyFit(): void
    {
        $broker = new ReadsBroker();
        $broker->write('tf.events', 'trace-1', self::payloads(3));
        $broker->write('tf.events', 'trace-2', [['a' => 1]]);

        $read = $this->queen($broker)->queue('tf.events')->fetch(['trace-1', 'trace-2']);

        $this->assertSame(['trace-1', 'trace-2'], array_keys($read));
        $t1 = $read['trace-1'];
        $this->assertSame(self::payloads(3), array_column($t1['records'], 'payload'));
        $this->assertSame(['offset' => 0, 'transactionId' => 'trace-1-0', 'payload' => ['n' => 0], 'ts' => ReadsBroker::TS], $t1['records'][0]);
        $this->assertSame(['trace-1', 3, 0, 3], [$t1['partition'], $t1['highWatermark'], $t1['logStartOffset'], $t1['nextOffset']]);
        $this->assertSame([['a' => 1]], array_column($read['trace-2']['records'], 'payload'));
        $this->assertSame([[
            'entries' => [
                ['queue' => 'tf.events', 'partition' => 'trace-1', 'offset' => 0, 'maxBytes' => 8 * self::MIB],
                ['queue' => 'tf.events', 'partition' => 'trace-2', 'offset' => 0, 'maxBytes' => 8 * self::MIB],
            ],
            'maxBytes' => 8 * self::MIB,
        ]], $broker->fetchBodies());
    }

    public function testFetchAsksAgainUntilTheHighWatermarkOfTheFirstAnswer(): void
    {
        $broker = new ReadsBroker();
        $broker->recordsPerEntry = 3;
        $broker->write('q', 'p', self::payloads(10));

        $read = $this->queen($broker)->queue('q')->fetch('p');

        $this->assertSame(range(0, 9), self::offsets($read['p']));
        $this->assertSame([0, 3, 6, 9], array_map(fn(array $b) => $b['entries'][0]['offset'], $broker->fetchBodies()));
        $this->assertSame(10, $read['p']['nextOffset']);
    }

    public function testFetchDoesNotChaseAPartitionWrittenDuringTheRead(): void
    {
        $broker = new ReadsBroker();
        $broker->recordsPerEntry = 2;
        $broker->write('q', 'p', self::payloads(4));
        $broker->afterFetch = fn(ReadsBroker $b) => $b->write('q', 'p', [['late' => true]]);

        $read = $this->queen($broker)->queue('q')->fetch(['p']);

        $this->assertSame([0, 1, 2, 3], self::offsets($read['p']));
        $this->assertCount(2, $broker->fetchBodies());
        $this->assertSame(4, $read['p']['nextOffset']);
    }

    public function testFetchAsksForAtMost1000PartitionsPerCall(): void
    {
        $broker = new ReadsBroker();
        $names = array_map(fn(int $i) => "p-{$i}", range(0, 2499));
        foreach ($names as $name) {
            $broker->write('q', $name, [['name' => $name]]);
        }

        $read = $this->queen($broker)->queue('q')->fetch($names);

        $this->assertSame([1000, 1000, 500], array_map(fn(array $b) => count($b['entries']), $broker->fetchBodies()));
        $this->assertCount(2500, $read);
        foreach ($names as $name) {
            $this->assertSame([['name' => $name]], array_column($read[$name]['records'], 'payload'));
        }
    }

    public function testFetchAsksAgainForThePartitionsTheCallCeilingLeftEmpty(): void
    {
        $broker = new ReadsBroker();
        $fat = str_repeat('x', 100);
        foreach (['a', 'b', 'c'] as $name) {
            $broker->write('q', $name, [$fat, $fat]);
        }

        $read = $this->queen($broker)->queue('q')->fetch(['a', 'b', 'c'], [], 250);

        foreach (['a', 'b', 'c'] as $name) {
            $this->assertSame([0, 1], self::offsets($read[$name]), $name);
        }
        $this->assertGreaterThan(1, count($broker->fetchBodies()));
        foreach ($broker->fetchBodies() as $body) {
            $this->assertSame(250, $body['maxBytes']);
        }
    }

    public function testFetchReadsFromLogStartWhenRetentionDeletedTheStart(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'p', self::payloads(10));
        $broker->trim('q', 'p', 6);

        $read = $this->queen($broker)->queue('q')->fetch(['p']);

        $this->assertSame([6, 7, 8, 9], self::offsets($read['p']));
        $this->assertSame(6, $read['p']['logStartOffset']);
        $this->assertSame([0, 6], array_map(fn(array $b) => $b['entries'][0]['offset'], $broker->fetchBodies()));
    }

    public function testFetchStartsEachPartitionAtItsOffset(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'a', self::payloads(5));
        $broker->write('q', 'b', self::payloads(5));
        $queue = $this->queen($broker)->queue('q');

        $read = $queue->fetch(['a', 'b'], ['a' => 3]);
        $this->assertSame([3, 4], self::offsets($read['a']));
        $this->assertSame([0, 1, 2, 3, 4], self::offsets($read['b']));

        $again = $queue->fetch(['a'], ['a' => $read['a']['nextOffset']]);
        $this->assertSame([], $again['a']['records']);
        $this->assertSame(5, $again['a']['nextOffset']);
    }

    public function testFetchAnswersNoRecordsForAMissingQueueAnUnwrittenPartitionAndAnOffsetPastTheEnd(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'p', self::payloads(2));
        $queen = $this->queen($broker);

        $this->assertSame([], $queen->queue('nope')->fetch(['p'])['p']['records']);
        $read = $queen->queue('q')->fetch(['never', 'p'], ['p' => 7]);
        $this->assertSame([], $read['never']['records']);
        $this->assertSame([], $read['p']['records']);
        $this->assertSame([7, 2], [$read['p']['nextOffset'], $read['p']['highWatermark']]);
    }

    public function testFetchThrowsOnAnErrorMarkerItDoesNotKnow(): void
    {
        $broker = new ReadsBroker();
        $broker->fetchOverride = [200, ['entries' => [['queue' => 'q', 'partition' => 'p', 'records' => [], 'highWatermark' => 1, 'logStartOffset' => 0, 'error' => 'SOMETHING_NEW']]]];

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('SOMETHING_NEW');
        $this->queen($broker)->queue('q')->fetch(['p']);
    }

    public function testFetchStopsWhenACallReadsNothingAlthoughThePartitionIsNotAtItsEnd(): void
    {
        $broker = new ReadsBroker();
        $broker->fetchOverride = [200, ['entries' => [['queue' => 'q', 'partition' => 'p', 'records' => [], 'highWatermark' => 5, 'logStartOffset' => 0]]]];

        $read = $this->queen($broker)->queue('q')->fetch(['p']);

        $this->assertSame([], $read['p']['records']);
        $this->assertCount(1, $broker->fetchBodies());
    }

    public function testFetchAsksOnceForANameGivenTwiceAndRefusesBadInputBeforeAnyCall(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'p', self::payloads(1));
        $queue = $this->queen($broker)->queue('q');

        $this->assertCount(1, $queue->fetch(['p', 'p']));
        $this->assertCount(1, $broker->fetchBodies()[0]['entries']);
        foreach ([fn() => $queue->fetch(['']), fn() => $queue->fetch(['p'], ['p' => -1]), fn() => $queue->fetch(['p'], [], 0)] as $bad) {
            try {
                $bad();
                $this->fail('bad input was accepted');
            } catch (\InvalidArgumentException) {
            }
        }
        $this->assertCount(1, $broker->fetchBodies());
    }

    public function testAPartitionNamedLikeANumberKeepsItsNameInItsResult(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', '3', [['x' => 1]]);

        $read = $this->queen($broker)->queue('q')->fetch(['3']);

        $this->assertSame('3', $read['3']['partition']);
        $this->assertSame('3', $broker->fetchBodies()[0]['entries'][0]['partition']);
    }

    public function testChangedSinceFollowsThePageCursorToTheEndOfThePass(): void
    {
        $broker = new ReadsBroker();
        for ($i = 0; $i < 2300; $i++) {
            $broker->write('tf.events', "t-{$i}", [['i' => $i]]);
        }

        $changed = $this->queen($broker)->queue('tf.events')->changedSince('2026-10-20T09:00:00Z');

        $this->assertCount(2300, $changed['partitions']);
        $this->assertSame(['name' => 't-0', 'id' => 'id-t-0', 'lastOffset' => 0, 'logStart' => 0, 'lastWriteAt' => ReadsBroker::TS], $changed['partitions'][0]);
        $this->assertSame('t-2299', $changed['partitions'][2299]['name']);
        $this->assertSame('2026-10-20T12:00:00.000000Z', $changed['safeTime']);
        $entries = array_map(fn(array $h) => $h['body']['entries'][0], $broker->hits);
        $this->assertSame([null, 'p|999', 'p|1999'], array_map(fn(array $e) => $e['after'] ?? null, $entries));
        foreach ($entries as $e) {
            $this->assertSame(['tf.events', '2026-10-20T09:00:00Z', 1000], [$e['queue'], $e['since'], $e['limit']]);
        }
    }

    public function testChangedSinceListsOnlyWhatWasWrittenAtOrAfterSince(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'old', [1], '2026-10-20T08:00:00.000000Z');
        $broker->write('q', 'new', [1], '2026-10-20T10:30:00.000000Z');

        $since = new \DateTimeImmutable('2026-10-20T12:00:00+02:00');
        $changed = $this->queen($broker)->queue('q')->changedSince($since);

        $this->assertSame(['new'], array_column($changed['partitions'], 'name'));
        $this->assertSame('2026-10-20T10:00:00.000000Z', $broker->hits[0]['body']['entries'][0]['since']);
    }

    public function testChangedSinceWithoutSinceListsEveryPartitionAndSendsNoSince(): void
    {
        $broker = new ReadsBroker();
        $broker->write('q', 'a', [1], '2020-01-01T00:00:00.000000Z');

        $changed = $this->queen($broker)->queue('q')->changedSince();

        $this->assertSame(['a'], array_column($changed['partitions'], 'name'));
        $this->assertArrayNotHasKey('since', $broker->hits[0]['body']['entries'][0]);
    }

    public function testChangedSinceAnswersTheSmallestSafeTimeOfItsPages(): void
    {
        $broker = new ReadsBroker();
        for ($i = 0; $i < 1500; $i++) {
            $broker->write('q', "p-{$i}", [1]);
        }
        $broker->safeTimes = ['2026-10-20T12:00:05.000000Z', '2026-10-20T12:00:01.000000Z'];

        $changed = $this->queen($broker)->queue('q')->changedSince(null);

        $this->assertSame('2026-10-20T12:00:01.000000Z', $changed['safeTime']);
    }

    public function testChangedSinceAnswersNothingForAMissingQueueAndThrowsOnBadCursor(): void
    {
        $broker = new ReadsBroker();
        $changed = $this->queen($broker)->queue('ghost')->changedSince('2026-10-20T00:00:00Z');
        $this->assertSame(['partitions' => [], 'safeTime' => '2026-10-20T12:00:00.000000Z'], $changed);

        $bad = new ReadsBroker();
        $bad->changedOverride = [200, ['safeTime' => '2026-10-20T12:00:00.000000Z', 'safeTimeDegraded' => false, 'entries' => [['queue' => 'q', 'error' => 'BAD_CURSOR']]]];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('BAD_CURSOR');
        $this->queen($bad)->queue('q')->changedSince(null);
    }
}
