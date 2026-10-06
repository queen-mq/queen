<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Queen;
use Queen\Tests\Support\PlanHandler;

final class AdminTest extends TestCase
{
    /**
     * The KV list route takes its cursor in the body: a cursor is a key, and
     * a key in a query string is written to every access log on the way.
     */
    public function testListKvPostsOnePageRequestToTheReadRoute(): void
    {
        $page = ['rows' => [['key' => 'jobs/v1/1', 'value' => ['n' => 1], 'version' => 1]], 'truncated' => false, 'nextAfter' => null, 'bytes' => 7];
        $handler = new PlanHandler([], ['status' => 200, 'json' => $page]);

        $result = $this->queen($handler)->admin()->listKv('queen-metrics', [
            'prefix' => 'jobs/v1/',
            'after' => 'jobs/v1/0',
            'limit' => 1000,
            'namespace' => 'other',
        ]);

        $this->assertSame($page, $result);
        $request = $handler->requests[0];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/api/v1/resources/kv/list', $request->getUri()->getPath());
        $this->assertSame('', $request->getUri()->getQuery());
        $this->assertSame(
            ['namespace' => 'queen-metrics', 'prefix' => 'jobs/v1/', 'after' => 'jobs/v1/0', 'limit' => 1000],
            json_decode((string) $request->getBody(), true),
            'the namespace argument wins over an option of the same name',
        );
    }

    /**
     * The 2.x broker has no route that dead-letters a message by its address:
     * the old POST answered 404 no_such_route. The method fails before any
     * request and names the ack that does it.
     */
    public function testMoveMessageToDlqThrowsAndNamesTheAckThatDeadLetters(): void
    {
        $handler = new PlanHandler([], ['status' => 404, 'json' => ['code' => 'no_such_route']]);

        try {
            $this->queen($handler)->admin()->moveMessageToDLQ('p1', 'tx-1');
            $this->fail('moveMessageToDLQ() must throw');
        } catch (\BadMethodCallException $e) {
            $this->assertStringContainsString("ack(\$message, 'dlq'", $e->getMessage());
        }

        $this->assertSame(0, $handler->count(), 'no request may leave the client');
    }

    private function queen(PlanHandler $handler): Queen
    {
        return new Queen(['url' => 'http://queen.test:6632', 'handler' => HandlerStack::create($handler)]);
    }
}
