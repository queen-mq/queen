<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Buffer\MessageBuffer;

class MessageBufferTest extends TestCase
{
    private array $flushedAddresses = [];

    private function makeBuffer(int $messageCount = 5, int $timeMillis = 1000): MessageBuffer
    {
        $this->flushedAddresses = [];

        return new MessageBuffer('test-queue/Default', [
            'messageCount' => $messageCount,
            'timeMillis' => $timeMillis,
        ], function (string $addr) {
            $this->flushedAddresses[] = $addr;
        });
    }

    public function testAddAndExtractMessages(): void
    {
        $buffer = $this->makeBuffer();

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']);
        $buffer->add(['payload' => 'msg3']);

        $this->assertSame(3, $buffer->getMessageCount());

        $messages = $buffer->extractMessages();
        $this->assertCount(3, $messages);
        $this->assertSame(0, $buffer->getMessageCount());
    }

    public function testExtractWithBatchSize(): void
    {
        $buffer = $this->makeBuffer();

        for ($i = 0; $i < 5; $i++) {
            $buffer->add(['payload' => "msg{$i}"]);
        }

        $batch1 = $buffer->extractMessages(2);
        $this->assertCount(2, $batch1);
        $this->assertSame(3, $buffer->getMessageCount());

        $batch2 = $buffer->extractMessages(2);
        $this->assertCount(2, $batch2);
        $this->assertSame(1, $buffer->getMessageCount());

        $batch3 = $buffer->extractMessages(2);
        $this->assertCount(1, $batch3);
        $this->assertSame(0, $buffer->getMessageCount());
    }

    public function testCountTriggerCallsFlush(): void
    {
        $buffer = $this->makeBuffer(3, 99999);

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']);
        $this->assertEmpty($this->flushedAddresses);

        $buffer->add(['payload' => 'msg3']); // Hits threshold of 3
        $this->assertCount(1, $this->flushedAddresses);
        $this->assertSame('test-queue/Default', $this->flushedAddresses[0]);
    }

    public function testRestoreMessagesPutsThemBack(): void
    {
        $buffer = $this->makeBuffer();

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']);

        $extracted = $buffer->extractMessages();
        $this->assertSame(0, $buffer->getMessageCount());

        $buffer->restoreMessages($extracted);
        $this->assertSame(2, $buffer->getMessageCount());

        // Messages should be in original order
        $restored = $buffer->extractMessages();
        $this->assertSame('msg1', $restored[0]['payload']);
        $this->assertSame('msg2', $restored[1]['payload']);
    }

    public function testRestorePrependsToExisting(): void
    {
        $buffer = $this->makeBuffer(100); // High threshold so no auto-flush

        $buffer->add(['payload' => 'existing1']);
        $buffer->add(['payload' => 'existing2']);

        $buffer->restoreMessages([['payload' => 'restored1'], ['payload' => 'restored2']]);

        $all = $buffer->extractMessages();
        $this->assertCount(4, $all);
        $this->assertSame('restored1', $all[0]['payload']);
        $this->assertSame('restored2', $all[1]['payload']);
        $this->assertSame('existing1', $all[2]['payload']);
        $this->assertSame('existing2', $all[3]['payload']);
    }

    public function testCleanupResetsState(): void
    {
        $buffer = $this->makeBuffer();

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']);
        $this->assertSame(2, $buffer->getMessageCount());

        $buffer->cleanup();
        $this->assertSame(0, $buffer->getMessageCount());
        $this->assertSame(0.0, $buffer->getFirstMessageAge());
    }

    public function testFirstMessageAgeTracking(): void
    {
        $buffer = $this->makeBuffer();

        $this->assertSame(0.0, $buffer->getFirstMessageAge());

        $buffer->add(['payload' => 'msg1']);
        usleep(10_000); // 10ms
        $age = $buffer->getFirstMessageAge();
        $this->assertGreaterThan(5, $age); // At least 5ms
    }

    public function testFlushingFlagPreventsDoubleTrigger(): void
    {
        $buffer = $this->makeBuffer(2, 99999);

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']); // Triggers flush, sets flushing=true

        // Now the buffer is in "flushing" state.
        // Adding more should NOT trigger again because flushing=true
        $buffer->add(['payload' => 'msg3']);
        $buffer->add(['payload' => 'msg4']);

        // Only one flush call
        $this->assertCount(1, $this->flushedAddresses);
    }

    public function testExtractResetsFlushingFlag(): void
    {
        $buffer = $this->makeBuffer(2, 99999);

        $buffer->add(['payload' => 'msg1']);
        $buffer->add(['payload' => 'msg2']); // Triggers flush

        // Extract clears the flushing flag
        $buffer->extractMessages();

        // Now adding should trigger flush again
        $buffer->add(['payload' => 'msg3']);
        $buffer->add(['payload' => 'msg4']); // Should trigger

        $this->assertCount(2, $this->flushedAddresses);
    }

    public function testGetOptions(): void
    {
        $buffer = $this->makeBuffer(42, 5000);
        $options = $buffer->getOptions();
        $this->assertSame(42, $options['messageCount']);
        $this->assertSame(5000, $options['timeMillis']);
    }
}
