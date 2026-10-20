<?php

namespace Queen\Reads;

use Queen\Http\HttpClient;

/**
 * Reads by offset and partition discovery, as two queue methods:
 *
 *     $read = $queen->queue('orders')->fetch(['customer-42', 'customer-7']);
 *     $read['customer-42']['records'];   // [['offset', 'transactionId', 'payload', 'ts'], ...]
 *
 *     $changed = $queen->queue('orders')->changedSince($since);
 *     $changed['partitions'], $changed['safeTime'];
 *
 * fetch is POST /api/v1/fetch and changedSince is POST /api/v1/partitions/changed.
 * Neither takes a lease, moves a cursor or needs an ack: two readers asking the
 * same question get the same answer, and no consumer group notices them. Each is
 * served from the answering node's own applied state.
 *
 * Both do the loops a caller of the raw routes has to write:
 * - fetch asks for at most 1000 partitions per call, and asks again from one
 *   past the last record until each partition reaches the high watermark its
 *   first answer reported. A partition whose start was deleted by retention is
 *   read from its oldest kept record (an offset below logStartOffset answers
 *   OFFSET_OUT_OF_RANGE and no records, so a loop that does not restart there
 *   silently reads nothing).
 * - changedSince follows the page cursor (next) until the broker says the pass
 *   is over, and answers the smallest safeTime of its pages.
 */
final class OffsetReads
{
    /** The broker refuses more than 1024 entries in one call; 1000 leaves room. */
    public const ENTRIES_PER_CALL = 1000;

    /**
     * Bytes of payload one call may carry: each entry's budget and the ceiling
     * of the whole call. 8 MiB is the most the broker reads for one entry.
     */
    public const DEFAULT_MAX_BYTES = 8 * 1024 * 1024;

    /** Partitions per discovery page: the broker's default and its ceiling. */
    public const CHANGED_PAGE = 1000;

    private const OFFSET_OUT_OF_RANGE = 'OFFSET_OUT_OF_RANGE';
    private const UNKNOWN_TOPIC_OR_PARTITION = 'UNKNOWN_TOPIC_OR_PARTITION';

    /**
     * Every call of one read goes to the same node: a follower answers from
     * what it has applied, so two nodes can disagree about where a partition
     * ends.
     */
    private static function affinity(string $queue): string
    {
        return "{$queue}:*:__READ__";
    }

    /**
     * Every record of $partitions, read by offset: from each partition's offset
     * in $offsets (default 0, the oldest record still kept) up to where the
     * partition ended when it was first read.
     *
     * Answers an array keyed by partition name, in the order the partitions
     * were named, of ['partition', 'records', 'highWatermark', 'logStartOffset',
     * 'nextOffset']. A partition nobody has written, or of a queue that does not
     * exist, answers no records. So does an offset past the end of the
     * partition ('nextOffset' above 'highWatermark' says so: the partition was
     * deleted and written again).
     *
     * @param string|array<int|string> $partitions
     * @param array<string,int> $offsets
     * @return array<string, array{partition: string, records: list<array{offset: int, transactionId: ?string, payload: mixed, ts: ?string}>, highWatermark: int, logStartOffset: int, nextOffset: int}>
     */
    public static function fetch(HttpClient $http, string $queue, string|array $partitions, array $offsets = [], int $maxBytes = self::DEFAULT_MAX_BYTES): array
    {
        if ($maxBytes < 1) {
            throw new \InvalidArgumentException("fetch: maxBytes must be a positive integer, got {$maxBytes}");
        }
        $names = [];
        foreach ((is_string($partitions) ? [$partitions] : $partitions) as $p) {
            $name = is_int($p) ? (string) $p : $p;
            if (!is_string($name) || $name === '') {
                throw new \InvalidArgumentException('fetch: a partition name must be a non-empty string, got ' . json_encode($p));
            }
            $names[$name] = true;
        }
        // Array keys: a name like "3" comes back as the integer 3.
        $names = array_map('strval', array_keys($names));

        $state = [];
        foreach ($names as $name) {
            $start = $offsets[$name] ?? 0;
            if (!is_int($start) || $start < 0) {
                throw new \InvalidArgumentException("fetch: offsets['{$name}'] must be a non-negative integer, got " . json_encode($start));
            }
            $state[$name] = [
                'partition' => $name,
                'records' => [],
                'highWatermark' => 0,
                'logStartOffset' => 0,
                'nextOffset' => $start,
                // The high watermark of the first answer: the read ends there.
                'end' => null,
            ];
        }

        $pending = $names;
        while ($pending !== []) {
            $again = [];
            foreach (array_chunk($pending, self::ENTRIES_PER_CALL) as $chunk) {
                $entries = [];
                foreach ($chunk as $name) {
                    $entries[] = [
                        'queue' => $queue,
                        'partition' => $name,
                        'offset' => $state[$name]['nextOffset'],
                        'maxBytes' => $maxBytes,
                    ];
                }
                $answer = $http->post('/api/v1/fetch', ['entries' => $entries, 'maxBytes' => $maxBytes], null, self::affinity($queue));
                $results = is_array($answer) ? ($answer['entries'] ?? null) : null;
                if (!is_array($results) || count($results) !== count($chunk)) {
                    $got = is_array($results) ? count($results) : 'no';
                    throw new \RuntimeException("fetch {$queue}: the broker answered {$got} entries for " . count($chunk) . ' asked');
                }

                $delivered = 0;
                // Entries that came back empty while short of their end: the
                // call's byte ceiling was spent by the entries before them.
                $starved = [];
                foreach ($chunk as $j => $name) {
                    $entry = $results[$j];
                    $s = &$state[$name];
                    if (isset($entry['highWatermark'])) {
                        $s['highWatermark'] = (int) $entry['highWatermark'];
                    }
                    if (isset($entry['logStartOffset'])) {
                        $s['logStartOffset'] = (int) $entry['logStartOffset'];
                    }
                    $error = $entry['error'] ?? null;
                    if ($error) {
                        if ($error === self::OFFSET_OUT_OF_RANGE && $s['nextOffset'] < $s['logStartOffset']) {
                            // Retention deleted what was asked for: go on from
                            // the oldest kept.
                            $s['nextOffset'] = $s['logStartOffset'];
                            $s['end'] ??= $s['highWatermark'];
                            if ($s['nextOffset'] < $s['end']) {
                                $again[] = $name;
                            }
                            unset($s);
                            continue;
                        }
                        if ($error === self::OFFSET_OUT_OF_RANGE || $error === self::UNKNOWN_TOPIC_OR_PARTITION) {
                            unset($s);
                            continue;
                        }
                        throw new \RuntimeException("fetch {$queue}/{$name}: the broker answered {$error}");
                    }
                    $s['end'] ??= $s['highWatermark'];
                    $records = is_array($entry['records'] ?? null) ? $entry['records'] : [];
                    if ($records !== []) {
                        $delivered += count($records);
                        foreach ($records as $r) {
                            $s['records'][] = [
                                'offset' => (int) $r['offset'],
                                'transactionId' => $r['transactionId'] ?? null,
                                'payload' => $r['payload'] ?? null,
                                'ts' => $r['ts'] ?? null,
                            ];
                        }
                        $s['nextOffset'] = (int) $records[count($records) - 1]['offset'] + 1;
                        if ($s['nextOffset'] < $s['end']) {
                            $again[] = $name;
                        }
                    } elseif ($s['nextOffset'] < $s['end']) {
                        $starved[] = $name;
                    }
                    unset($s);
                }
                // An empty entry is asked again only when its call read
                // something: a call that read nothing at all would answer the
                // same next time.
                if ($delivered > 0) {
                    array_push($again, ...$starved);
                }
            }
            $pending = $again;
        }

        $out = [];
        foreach ($names as $name) {
            unset($state[$name]['end']);
            $out[$name] = $state[$name];
        }
        return $out;
    }

    /**
     * The partitions of the queue written at or after $since, in the order they
     * were created, and the safeTime of the answer: ['partitions' => [...],
     * 'safeTime' => ...]. Each partition is ['name', 'id', 'lastOffset',
     * 'logStart', 'lastWriteAt']; 'lastOffset' is the offset of its last record
     * (-1 when none).
     *
     * $since is a DateTimeInterface or an ISO-8601 string; null lists every
     * partition of the queue. A queue that does not exist has none.
     *
     * safeTime is the broker's time up to which every write is counted in this
     * answer: pass it as the next $since to see only what changed after this
     * call. It comes from the broker's clock, never compare it with the local
     * one.
     *
     * @return array{partitions: list<array{name: string, id: ?string, lastOffset: int, logStart: int, lastWriteAt: string}>, safeTime: ?string}
     */
    public static function changedSince(HttpClient $http, string $queue, \DateTimeInterface|string|null $since = null): array
    {
        if ($since instanceof \DateTimeInterface) {
            $since = \DateTimeImmutable::createFromInterface($since)
                ->setTimezone(new \DateTimeZone('UTC'))
                ->format('Y-m-d\TH:i:s.u\Z');
        } elseif ($since === '') {
            throw new \InvalidArgumentException('changedSince: since must be a time; pass null for every partition');
        }

        $partitions = [];
        $safeTime = null;
        $after = null;
        while (true) {
            $entry = ['queue' => $queue, 'limit' => self::CHANGED_PAGE];
            if ($since !== null) {
                $entry['since'] = $since;
            }
            if ($after !== null) {
                $entry['after'] = $after;
            }
            $answer = $http->post('/api/v1/partitions/changed', ['entries' => [$entry]], null, self::affinity($queue));
            $answer = is_array($answer) ? $answer : [];
            // Pages served by different nodes: the smallest is the one that
            // holds for all of them. The broker spells every one the same way,
            // so the strings compare in time order.
            $pageSafe = $answer['safeTime'] ?? null;
            if (is_string($pageSafe) && ($safeTime === null || strcmp($pageSafe, $safeTime) < 0)) {
                $safeTime = $pageSafe;
            }
            $result = $answer['entries'][0] ?? null;
            if (!is_array($result)) {
                throw new \RuntimeException("changedSince {$queue}: the broker answered no entry");
            }
            $error = $result['error'] ?? null;
            if ($error === self::UNKNOWN_TOPIC_OR_PARTITION) {
                break;
            }
            if ($error) {
                throw new \RuntimeException("changedSince {$queue}: the broker answered {$error}");
            }
            foreach ($result['partitions'] ?? [] as $p) {
                $partitions[] = [
                    'name' => (string) $p['name'],
                    'id' => $p['id'] ?? null,
                    'lastOffset' => (int) $p['lastOffset'],
                    'logStart' => (int) $p['logStart'],
                    'lastWriteAt' => (string) $p['lastWriteAt'],
                ];
            }
            $next = $result['next'] ?? null;
            if (!is_string($next) || $next === '') {
                break;
            }
            $after = $next;
        }
        return ['partitions' => $partitions, 'safeTime' => $safeTime];
    }
}
