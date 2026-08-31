<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Consumer\HighLevelConsumer;
use Queen\Http\HttpClient;
use Queen\Queen;

class HighLevelConsumerTest extends TestCase
{
    public function testConsumeWithoutSubscribeThrows(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('not subscribed');
        $consumer->consume();
    }

    public function testConsumeReturnsMessageFromServer(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->expects($this->once())
            ->method('get')
            ->willReturn([
                'messages' => [
                    ['transactionId' => 'tx-1', 'partitionId' => 'p1', 'payload' => ['data' => 1]],
                ],
            ]);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $msg = $consumer->consume(1000);

        $this->assertNotNull($msg);
        $this->assertSame('tx-1', $msg['transactionId']);
        $this->assertArrayHasKey('trace', $msg);
    }

    public function testConsumeReturnsNullOnEmpty(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->method('get')->willReturn(['messages' => []]);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $msg = $consumer->consume(100);

        $this->assertNull($msg);
    }

    public function testConsumeReturnsNullOnTimeout(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->method('get')->willThrowException(new \RuntimeException('cURL error 28: Operation timed out'));

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $msg = $consumer->consume(100);

        $this->assertNull($msg);
    }

    public function testAckDelegatesToQueen(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createMock(Queen::class);

        $message = ['transactionId' => 'tx-1', 'partitionId' => 'p1'];

        $queen->expects($this->once())
            ->method('ack')
            ->with($message, true, ['group' => 'g1'])
            ->willReturn(['success' => true]);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $result = $consumer->ack($message);
        $this->assertTrue($result['success']);
    }

    public function testNackDelegatesToQueen(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createMock(Queen::class);

        $message = ['transactionId' => 'tx-1', 'partitionId' => 'p1'];

        $queen->expects($this->once())
            ->method('ack')
            ->with($message, false, ['group' => 'g1'])
            ->willReturn(['success' => true]);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->nack($message);
    }

    public function testCloseStopsConsuming(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        // get() should never be called after close
        $httpClient->expects($this->never())->method('get');

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $consumer->close();

        $this->assertTrue($consumer->isClosed());
        $msg = $consumer->consume(100);
        $this->assertNull($msg);
    }

    public function testConsumeBatchReturnsBatch(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->expects($this->once())
            ->method('get')
            ->willReturn([
                'messages' => [
                    ['transactionId' => 'tx-1', 'partitionId' => 'p1', 'payload' => 1],
                    ['transactionId' => 'tx-2', 'partitionId' => 'p2', 'payload' => 2],
                    ['transactionId' => 'tx-3', 'partitionId' => 'p3', 'payload' => 3],
                ],
            ]);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $messages = $consumer->consumeBatch(1000, 10);

        $this->assertCount(3, $messages);
        $this->assertSame('tx-1', $messages[0]['transactionId']);
        $this->assertSame('tx-3', $messages[2]['transactionId']);
    }

    public function testRenewLeaseDelegatesToQueen(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createMock(Queen::class);

        $queen->expects($this->once())
            ->method('renew')
            ->with('lease-123', 120)
            ->willReturn(['success' => true, 'leaseId' => 'lease-123']);

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $result = $consumer->renewLease('lease-123', 120);
        $this->assertTrue($result['success']);
    }

    public function testConsumeReturnsNullOnConnectionRefused(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->method('get')->willThrowException(new \RuntimeException('Connection refused'));

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();
        $msg = $consumer->consume(100);

        $this->assertNull($msg);
    }

    public function testConsumeThrowsOnUnexpectedError(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $queen = $this->createStub(Queen::class);

        $httpClient->method('get')->willThrowException(new \RuntimeException('Unexpected error'));

        $consumer = new HighLevelConsumer($httpClient, $queen, [
            'queue' => 'test',
            'group' => 'g1',
        ]);

        $consumer->subscribe();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Unexpected error');
        $consumer->consume(100);
    }
}
