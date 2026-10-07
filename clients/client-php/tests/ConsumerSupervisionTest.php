<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Queen\Consumer\Supervision;
use Queen\Http\HttpClient;
use Queen\Queen;

final class ConsumerSupervisionTest extends TestCase
{
    private function client(array &$documents, array &$paths, bool $refuse = false): Queen
    {
        $handler = function ($request, $options) use (&$documents, &$paths, $refuse) {
            $path = $request->getUri()->getPath();
            $paths[] = $path;
            if ($path === '/api/v1/kv') {
                self::assertSame('Bearer test-token', $request->getHeaderLine('Authorization'));
                self::assertSame(2, $options['timeout']);
                $ops = json_decode((string) $request->getBody(), true)['operations'];
                self::assertCount(2, $ops);
                [$head, $chunk] = $ops;
                self::assertSame('queen-supervisor', $head['ns']);
                self::assertSame($head['ttlSeconds'], $chunk['ttlSeconds']);
                self::assertSame($head['value']['write'], $chunk['value']['write']);
                $raw = base64_decode($chunk['value']['data'], true);
                self::assertSame(strlen($raw), $head['value']['bytes']);
                self::assertStringNotContainsString('private', $raw);
                $doc = json_decode($raw, true);
                self::assertGreaterThanOrEqual(2 * $doc['configuration']['heartbeat_timeout'], $head['ttlSeconds']);
                $documents[] = $doc;
                return new FulfilledPromise(new Response($refuse ? 429 : 200, ['Retry-After' => '1000'], json_encode(['results' => [['applied' => true], ['applied' => true]]])));
            }
            return new FulfilledPromise(new Response(200, [], json_encode(['messages' => [[
                'transactionId' => 't', 'partitionId' => 'p', 'leaseId' => 'lease', 'data' => ['private' => 'payload'],
            ]]])));
        };
        return new Queen(['url' => 'http://plan.local', 'bearerToken' => 'test-token', 'handler' => HandlerStack::create($handler)]);
    }

    public function testDisabledByDefaultAndExplicitNull(): void
    {
        $docs = $paths = [];
        $client = $this->client($docs, $paths);
        foreach ([false, true] as $explicit) {
            $builder = $client->queue('orders')->wait(false)->autoAck(false)->limit(1);
            if ($explicit) { $builder->supervision(null); }
            $builder->consume(static function (): void {})->execute();
        }
        self::assertSame([], $docs);
        self::assertCount(2, $paths);
    }

    public function testCooperativeLifecycleCountsAndFirstBusyHandler(): void
    {
        $docs = $paths = [];
        $client = $this->client($docs, $paths);
        $client->queue('orders')->supervision(['group' => 'billing-production'])
            ->concurrency(2)->limit(2)->each()->autoAck(false)->wait(false)
            ->consume(static function (): void {})->execute();
        self::assertSame('cooperative', $docs[0]['execution_model']);
        self::assertSame(45, $docs[0]['configuration']['heartbeat_timeout']);
        self::assertSame(1, $docs[1]['pool_status'][0]['busy']);
        $last = $docs[array_key_last($docs)];
        self::assertSame('stopped', $last['state']);
        self::assertSame(0, $last['pool_status'][0]['running']);
        self::assertSame(0, $last['pool_status'][0]['busy']);
        self::assertSame(2, $last['pool_status'][0]['completed']);
        self::assertSame(0, $last['pool_status'][0]['failed']);
        self::assertNotNull($last['pool_status'][0]['last_completed_at_epoch']);
        self::assertNotContains('/api/v1/ack', $paths);
    }

    public function testRefusedPublicationNeverRetriesOrChangesTheHandlerError(): void
    {
        $docs = $paths = [];
        $client = $this->client($docs, $paths, true);
        $error = new \RuntimeException('private failure');
        $started = microtime(true);
        try {
            $client->queue('orders')->supervision(['group' => 'billing'])->autoAck(false)->wait(false)
                ->consume(static function () use ($error): void { throw $error; })->execute();
            self::fail('The handler error must propagate');
        } catch (\RuntimeException $actual) {
            self::assertSame($error, $actual);
        }
        self::assertLessThan(1, microtime(true) - $started, 'A 429 must not invoke Retry-After sleep');
        self::assertCount(3, $docs, 'One attempt each for initial, first handler and stopped');
        self::assertSame('stopped', $docs[2]['state']);
        self::assertSame(1, $docs[2]['pool_status'][0]['failed']);
    }

    public function testInvalidGroupsAndUniqueInstances(): void
    {
        $http = $this->createStub(HttpClient::class);
        $first = new Supervision($http, ['group' => 'billing'], ['concurrency' => 1, 'queue' => '0', 'group' => '0']);
        self::assertSame('0', $first->document()['pool_status'][0]['queue']);
        self::assertSame('0', $first->document()['pool_status'][0]['consumer_group']);
        $second = new Supervision($http, ['group' => 'billing'], ['concurrency' => 1]);
        self::assertNotSame($first->document()['instance_id'], $second->document()['instance_id']);
        foreach (['', 'coordination', 'a/b', "a\n"] as $group) {
            try { new Supervision($http, ['group' => $group], []); self::fail('Invalid group accepted'); }
            catch (\InvalidArgumentException) {}
        }
    }
}
