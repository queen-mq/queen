<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Builders\TransactionBuilder;
use Queen\Http\HttpClient;

class TransactionBuilderTest extends TestCase
{
    public function testCommitWithNoOperationsThrows(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $tx = new TransactionBuilder($httpClient);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Transaction has no operations');
        $tx->commit();
    }

    public function testAckWithoutTransactionIdThrows(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $tx = new TransactionBuilder($httpClient);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('transactionId');
        $tx->ack([['partitionId' => 'p1']]);
    }

    public function testAckWithoutPartitionIdThrows(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $tx = new TransactionBuilder($httpClient);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('partitionId');
        $tx->ack([['transactionId' => 'tx-1']]);
    }

    public function testAckAndPushBuildOperations(): void
    {
        $httpClient = $this->createMock(HttpClient::class);

        $httpClient->expects($this->once())
            ->method('post')
            ->with(
                '/api/v1/transaction',
                $this->callback(function (array $body) {
                    // Should have 2 operations: 1 ack + 1 push
                    $this->assertCount(2, $body['operations']);
                    $this->assertSame('ack', $body['operations'][0]['type']);
                    $this->assertSame('tx-1', $body['operations'][0]['transactionId']);
                    $this->assertSame('p1', $body['operations'][0]['partitionId']);
                    $this->assertSame('completed', $body['operations'][0]['status']);
                    $this->assertSame('push', $body['operations'][1]['type']);
                    $this->assertCount(1, $body['operations'][1]['items']);
                    return true;
                })
            )
            ->willReturn(['success' => true, 'transactionId' => 'abc']);

        $tx = new TransactionBuilder($httpClient);

        $result = $tx
            ->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1', 'leaseId' => 'lease-1']])
            ->queue('notifications')->push([['data' => ['notify' => true]]])
            ->commit();

        $this->assertTrue($result['success']);
    }

    public function testCommitFailureIncludesTransactionId(): void
    {
        $httpClient = $this->createStub(HttpClient::class);
        $httpClient->method('post')->willReturn([
            'success' => false,
            'error' => 'conflict',
            'transactionId' => 'tx-abc',
        ]);

        $tx = new TransactionBuilder($httpClient);
        $tx->ack([['transactionId' => 'tx-1', 'partitionId' => 'p1']]);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('tx-abc');
        $tx->commit();
    }

    public function testQueueWithPartition(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $httpClient->expects($this->once())
            ->method('post')
            ->with(
                '/api/v1/transaction',
                $this->callback(function (array $body) {
                    $pushOp = $body['operations'][0];
                    $this->assertSame('push', $pushOp['type']);
                    $this->assertSame('user-123', $pushOp['items'][0]['partition']);
                    return true;
                })
            )
            ->willReturn(['success' => true]);

        $tx = new TransactionBuilder($httpClient);
        $tx->queue('orders')->partition('user-123')->push([['data' => ['test' => 1]]]);
        $tx->commit();
    }

    public function testLeaseIdsCollected(): void
    {
        $httpClient = $this->createMock(HttpClient::class);
        $httpClient->expects($this->once())
            ->method('post')
            ->with(
                '/api/v1/transaction',
                $this->callback(function (array $body) {
                    $this->assertContains('lease-1', $body['requiredLeases']);
                    $this->assertContains('lease-2', $body['requiredLeases']);
                    // Should deduplicate
                    $this->assertCount(2, $body['requiredLeases']);
                    return true;
                })
            )
            ->willReturn(['success' => true]);

        $tx = new TransactionBuilder($httpClient);
        $tx->ack([
            ['transactionId' => 'tx-1', 'partitionId' => 'p1', 'leaseId' => 'lease-1'],
            ['transactionId' => 'tx-2', 'partitionId' => 'p2', 'leaseId' => 'lease-2'],
            ['transactionId' => 'tx-3', 'partitionId' => 'p3', 'leaseId' => 'lease-1'], // duplicate
        ]);
        $tx->commit();
    }
}
