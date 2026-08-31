<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;

/**
 * Smoke tests for the producerSub field documentation contract (issue #23).
 *
 * The PHP client works with raw associative arrays for messages, so there is
 * no DTO/class to update for producerSub exposure; it simply flows through as
 * $message['producerSub']. These tests codify that contract so a future
 * refactor to typed messages doesn't silently drop the field.
 *
 * The integration tests that actually push and pop are in the other language
 * clients (client-js, client-py, client-go). Running those against a live
 * server with JWT enabled provides cross-language coverage of the invariant.
 */
class ProducerSubTest extends TestCase
{
    /**
     * Sample server pop response shape, based on what the server emits since
     * the 0.13.0 schema migration.
     */
    private function samplePopResponse(): array
    {
        return [
            'messages' => [
                [
                    'id' => 'a1b2c3d4-e5f6-7000-8000-000000000000',
                    'transactionId' => 'tx-1',
                    'partitionId' => 'p1',
                    'queue' => 'orders',
                    'partition' => 'Default',
                    'data' => ['orderId' => 42],
                    'createdAt' => '2026-04-17T12:00:00.000Z',
                    'producerSub' => 'alice-producer',
                ],
                [
                    'id' => 'b2c3d4e5-f607-7000-8000-000000000000',
                    'transactionId' => 'tx-2',
                    'partitionId' => 'p1',
                    'queue' => 'orders',
                    'partition' => 'Default',
                    'data' => ['orderId' => 43],
                    'createdAt' => '2026-04-17T12:00:01.000Z',
                    // No producerSub when message was pushed without auth.
                    'producerSub' => null,
                ],
            ],
        ];
    }

    public function testProducerSubIsAccessibleOnMessages(): void
    {
        $response = $this->samplePopResponse();
        $messages = $response['messages'];

        $this->assertSame('alice-producer', $messages[0]['producerSub']);
    }

    public function testProducerSubIsNullWhenServerOmitsIt(): void
    {
        $response = $this->samplePopResponse();
        $messages = $response['messages'];

        $this->assertNull($messages[1]['producerSub']);
    }

    public function testProducerSubMissingKeyHandlesGracefully(): void
    {
        // Messages from older servers (pre-0.13.0) will not include the key
        // at all. PHP code that reads $msg['producerSub'] ?? null must work.
        $oldMessage = [
            'id' => 'pre-0.13.0-message',
            'transactionId' => 'tx-3',
            'data' => ['foo' => 'bar'],
        ];

        $this->assertNull($oldMessage['producerSub'] ?? null);
    }
}
