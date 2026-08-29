<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;
use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Exceptions\HttpException;
use Queen\Tests\Support\PlanHandler;

class HttpClientTest extends TestCase
{
    public function testNoBaseUrlAndNoLoadBalancerThrows(): void
    {
        $client = new HttpClient([]);

        $this->expectException(\LogicException::class);
        $client->get('/test');
    }

    public function testHttpExceptionHasStatusCode(): void
    {
        $ex = new HttpException('Not Found', 404);

        $this->assertSame(404, $ex->statusCode);
        $this->assertSame('Not Found', $ex->getMessage());
        $this->assertInstanceOf(\RuntimeException::class, $ex);
    }

    public function testHttpExceptionWithPrevious(): void
    {
        $previous = new \RuntimeException('original');
        $ex = new HttpException('Wrapped', 500, 0, $previous);

        $this->assertSame(500, $ex->statusCode);
        $this->assertSame($previous, $ex->getPrevious());
    }

    public function testHttpExceptionKeepsServerErrorSeparateFromProxyCode(): void
    {
        $handler = new PlanHandler([], ['status' => 400, 'json' => [
            'error' => 'unsupported',
            'code' => 'operation_rejected',
            'reason' => 'timer_count_mode',
        ]]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen.test:6632',
            'handler' => HandlerStack::create($handler),
        ]);

        try {
            $client->get('/api/v1/timers/q?mode=count&prefix=laravel%3A');
            $this->fail('HTTP 400 must throw.');
        } catch (HttpException $exception) {
            $this->assertSame('unsupported', $exception->serverError);
            $this->assertSame('operation_rejected', $exception->errorCode);
            $this->assertSame('timer_count_mode', $exception->reason);
        }
    }

    public function testGetLoadBalancerReturnsNull(): void
    {
        $client = new HttpClient(['baseUrl' => 'http://localhost']);
        $this->assertNull($client->getLoadBalancer());
    }

    public function testGetLoadBalancerReturnsInstance(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b']);
        $client = new HttpClient(['loadBalancer' => $lb]);
        $this->assertSame($lb, $client->getLoadBalancer());
    }

    public function testAsyncFailoverMovesAReadAfterAServerError(): void
    {
        $handler = new PlanHandler([
            ['status' => 503, 'json' => ['error' => 'unavailable']],
            ['status' => 200, 'json' => ['pending' => 7]],
        ]);
        $loadBalancer = new LoadBalancer(['http://queen-a:6632', 'http://queen-b:6632'], 'round-robin');
        $client = new HttpClient([
            'loadBalancer' => $loadBalancer,
            'enableFailover' => true,
            'handler' => HandlerStack::create($handler),
        ]);

        $result = $client->getAsyncWithFailover('/depth')->wait();

        $this->assertSame(['pending' => 7], $result);
        $this->assertSame(['queen-a', 'queen-b'], $handler->hosts());
        $this->assertFalse($loadBalancer->getHealthStatus()['http://queen-a:6632']['healthy']);
    }

    public function testAsyncFailoverDoesNotForwardCredentialsAcrossARedirect(): void
    {
        $handler = new PlanHandler([
            ['status' => 302, 'json' => []],
        ]);
        $client = new HttpClient([
            'baseUrl' => 'http://queen-a:6632',
            'bearerToken' => 'read-secret',
            'handler' => HandlerStack::create($handler),
        ]);

        $this->assertSame([], $client->getAsyncWithFailover('/depth')->wait());
        $this->assertFalse($handler->options[0]['allow_redirects']);
        $this->assertSame('Bearer read-secret', $handler->requests[0]->getHeaderLine('Authorization'));
        $this->assertSame(1, $handler->count());
    }
}
