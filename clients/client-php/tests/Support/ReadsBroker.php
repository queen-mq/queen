<?php

namespace Queen\Tests\Support;

use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\RequestInterface;

/**
 * A Guzzle handler that answers POST /api/v1/fetch and
 * POST /api/v1/partitions/changed the way
 * server/src/rsm/facade/real/phase2/reads.rs does: entries answered in order,
 * each entry's byte budget and the whole call's ceiling (the first record of an
 * entry read whatever it weighs), a record cap per entry, OFFSET_OUT_OF_RANGE
 * below logStartOffset and above the high watermark, UNKNOWN_TOPIC_OR_PARTITION
 * for a queue that does not exist, discovery pages with a `next` cursor exactly
 * when a page is full, BAD_CURSOR for a cursor it did not issue.
 *
 * Every request is recorded with its decoded body.
 */
class ReadsBroker
{
    public const TS = '2026-10-20T10:00:00.000000Z';

    /** @var array<string, array<string, array{records: list<array>, logStart: int, created: int, lastWriteAt: string}>> */
    public array $queues = [];
    public int $recordsPerEntry = 10000;
    /** @var list<string> */
    public array $safeTimes = [];
    /** @var list<array{path: string, body: mixed}> */
    public array $hits = [];
    /** @var (callable(ReadsBroker): void)|null */
    public $afterFetch = null;
    public ?array $fetchOverride = null;
    public ?array $changedOverride = null;
    private int $created = 0;

    public function write(string $queue, string $partition, array $payloads, string $lastWriteAt = self::TS): void
    {
        if (!isset($this->queues[$queue][$partition])) {
            $this->queues[$queue][$partition] = ['records' => [], 'logStart' => 0, 'created' => $this->created++, 'lastWriteAt' => $lastWriteAt];
        }
        $p = &$this->queues[$queue][$partition];
        foreach ($payloads as $payload) {
            $offset = $p['records'] === [] ? $p['logStart'] : $p['records'][count($p['records']) - 1]['offset'] + 1;
            $p['records'][] = ['offset' => $offset, 'transactionId' => "{$partition}-{$offset}", 'payload' => $payload, 'ts' => self::TS];
        }
        $p['lastWriteAt'] = $lastWriteAt;
    }

    public function trim(string $queue, string $partition, int $offset): void
    {
        $p = &$this->queues[$queue][$partition];
        $p['records'] = array_values(array_filter($p['records'], fn(array $r) => $r['offset'] >= $offset));
        $p['logStart'] = $offset;
    }

    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $body = json_decode((string) $request->getBody(), true);
        $path = $request->getUri()->getPath();
        $this->hits[] = ['path' => $path, 'body' => $body];
        if ($request->getMethod() === 'POST' && $path === '/api/v1/fetch') {
            [$status, $answer] = $this->fetchOverride ?? $this->fetch($body);
            if ($this->afterFetch !== null) {
                ($this->afterFetch)($this);
            }
        } elseif ($request->getMethod() === 'POST' && $path === '/api/v1/partitions/changed') {
            [$status, $answer] = $this->changedOverride ?? $this->changed($body);
        } else {
            [$status, $answer] = [404, ['error' => 'not found']];
        }
        return new FulfilledPromise(new Response($status, ['Content-Type' => 'application/json'], json_encode($answer)));
    }

    /** @return list<array> */
    public function fetchBodies(): array
    {
        return array_values(array_map(fn(array $h) => $h['body'], array_filter($this->hits, fn(array $h) => $h['path'] === '/api/v1/fetch')));
    }

    private function fetch(array $body): array
    {
        if (count($body['entries']) > 1024) {
            return [400, ['error' => 'too many entries']];
        }
        $ceiling = $body['maxBytes'] ?? null;
        $used = 0;
        $entries = [];
        foreach ($body['entries'] as $e) {
            $partition = $e['partition'] ?? 'Default';
            $out = ['queue' => $e['queue'], 'partition' => $partition, 'records' => [], 'highWatermark' => 0, 'logStartOffset' => 0];
            if (!isset($this->queues[$e['queue']])) {
                $entries[] = $out + ['error' => 'UNKNOWN_TOPIC_OR_PARTITION'];
                continue;
            }
            $p = $this->queues[$e['queue']][$partition] ?? null;
            if ($p === null) {
                $entries[] = $out;
                continue;
            }
            $high = $p['records'] === [] ? $p['logStart'] : $p['records'][count($p['records']) - 1]['offset'] + 1;
            $out['highWatermark'] = $high;
            $out['logStartOffset'] = $p['logStart'];
            if ($e['offset'] < $p['logStart'] || $e['offset'] > $high) {
                $entries[] = $out + ['error' => 'OFFSET_OUT_OF_RANGE'];
                continue;
            }
            if ($ceiling !== null && $used >= $ceiling) {
                $entries[] = $out;
                continue;
            }
            $budget = min($e['maxBytes'] ?? 1048576, $ceiling === null ? PHP_INT_MAX : $ceiling - $used);
            $spent = 0;
            foreach ($p['records'] as $r) {
                if ($r['offset'] < $e['offset']) {
                    continue;
                }
                if (count($out['records']) >= $this->recordsPerEntry) {
                    break;
                }
                $weight = max(strlen(json_encode($r['payload'])), 1);
                if ($out['records'] !== [] && $spent + $weight > $budget) {
                    break;
                }
                $spent += $weight;
                $out['records'][] = $r;
            }
            $used += $spent;
            $entries[] = $out;
        }
        return [200, ['entries' => $entries]];
    }

    private function changed(array $body): array
    {
        $safeTime = count($this->safeTimes) > 1 ? array_shift($this->safeTimes) : ($this->safeTimes[0] ?? '2026-10-20T12:00:00.000000Z');
        $entries = [];
        foreach ($body['entries'] as $e) {
            $queue = $e['queue'];
            if (!isset($this->queues[$queue])) {
                $entries[] = ['queue' => $queue, 'error' => 'UNKNOWN_TOPIC_OR_PARTITION'];
                continue;
            }
            $from = 0;
            if (isset($e['after'])) {
                if (!preg_match('/^p\|(\d+)$/', $e['after'], $m)) {
                    $entries[] = ['queue' => $queue, 'error' => 'BAD_CURSOR'];
                    continue;
                }
                $from = (int) $m[1] + 1;
            }
            $limit = max(1, min(1000, $e['limit'] ?? 1000));
            $all = $this->queues[$queue];
            uasort($all, fn(array $a, array $b) => $a['created'] <=> $b['created']);
            $partitions = [];
            $last = 0;
            foreach ($all as $name => $p) {
                if ($p['created'] < $from || (isset($e['since']) && strcmp($p['lastWriteAt'], $e['since']) < 0)) {
                    continue;
                }
                $lastOffset = $p['records'] === [] ? $p['logStart'] - 1 : $p['records'][count($p['records']) - 1]['offset'];
                $partitions[] = ['name' => (string) $name, 'id' => "id-{$name}", 'lastOffset' => $lastOffset, 'logStart' => $p['logStart'], 'lastWriteAt' => $p['lastWriteAt']];
                $last = $p['created'];
                if (count($partitions) >= $limit) {
                    break;
                }
            }
            $entries[] = ['queue' => $queue, 'partitions' => $partitions, 'next' => count($partitions) >= $limit ? "p|{$last}" : null];
        }
        return [200, ['safeTime' => $safeTime, 'safeTimeDegraded' => false, 'entries' => $entries]];
    }
}
