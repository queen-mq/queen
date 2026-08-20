<?php

namespace Queen\Buffer;

use Queen\Http\HttpClient;
use Queen\Support\Defaults;

class BufferManager
{
    private HttpClient $httpClient;
    /** @var array<string, MessageBuffer> */
    private array $buffers = [];
    private int $flushCount = 0;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    public function addMessage(string $queueAddress, array $formattedMessage, array $bufferOptions): void
    {
        if (!isset($this->buffers[$queueAddress])) {
            $this->buffers[$queueAddress] = new MessageBuffer(
                $queueAddress,
                MessageBuffer::normalizeOptions($bufferOptions),
                fn(string $addr) => $this->doFlush($addr)
            );
        }

        $buffer = $this->buffers[$queueAddress];

        // Check time-based trigger before adding (since PHP has no background
        // timers). The same absence is why the maxSize bound inside add() is
        // enforced by flushing inline instead of parking: there is no other
        // thread that could drain the buffer while this one waits.
        $buffer->checkTimeTrigger();

        // May throw: the bound refuses to grow past maxSize, and the flush this
        // triggers rethrows if it cannot land inside its deadline. Either way
        // the message is UNCONFIRMED and the caller is told so, instead of the
        // pre-2026-08-20 behavior of appending to an unbounded array and
        // reporting success for messages that only lived in this process.
        $buffer->add($formattedMessage);
    }

    /**
     * Flush callback handed to every MessageBuffer: invoked inline, on the add
     * path, when the count trigger fires or the buffer hits its bound.
     */
    private function doFlush(string $queueAddress): void
    {
        $buffer = $this->buffers[$queueAddress] ?? null;
        if ($buffer === null || $buffer->getMessageCount() === 0) {
            return;
        }

        // NOTE: a drained buffer is deliberately kept in $buffers rather than
        // retired here. Retiring it mid-add orphaned the object the caller was
        // still holding — MessageBuffer::add() would append to a buffer no
        // longer reachable from $buffers, so that message was invisible to
        // getStats(), never flushed, and silently lost on the next add. Only
        // the explicit end-of-life paths (flushBuffer/flushAllBuffers/cleanup)
        // retire buffers. Keeping it also keeps the buffer's own options alive
        // instead of re-deriving them from whichever caller pushes next.
        $this->sendUntilDrained($queueAddress, $buffer);
    }

    /**
     * Send a buffer's messages in batches of messageCount, retrying a batch
     * that failed to send after retryDelayMillis, until the buffer drains or
     * the maxWaitMillis deadline expires.
     *
     * This retry loop IS the "block until capacity frees" of the Go/JS/Rust
     * SDKs, rewritten for a runtime with no background flusher: there is no
     * other thread making progress, so the only useful thing a full buffer can
     * do is keep trying to send, here, on the caller's stack.
     *
     * A failed batch goes back to the FRONT of the buffer, in order, before
     * anything else happens — it is never dropped and the failure is never
     * swallowed. When the deadline expires the original transport error is
     * rethrown with every message still queued: the caller learns the messages
     * were not accepted, and can retry, persist them, or fail the request.
     *
     * The deadline bounds the retry loop and is checked BETWEEN attempts; a
     * single in-flight POST is bounded by the HTTP client's own timeoutMillis.
     */
    private function sendUntilDrained(
        string $queueAddress,
        MessageBuffer $buffer,
        bool $retireWhenDrained = false
    ): void {
        $options = $buffer->getOptions();
        $batchSize = $options['messageCount'];
        $retryDelayMicros = (int) ($options['retryDelayMillis'] * 1000);
        $deadline = microtime(true) + ($options['maxWaitMillis'] / 1000);

        while ($buffer->getMessageCount() > 0) {
            $buffer->setFlushing(true);
            $messages = $buffer->extractMessages($batchSize);
            if (empty($messages)) {
                break;
            }

            try {
                $this->httpClient->post('/api/v1/push', ['items' => $messages]);
                $this->flushCount++;
            } catch (\Throwable $error) {
                $buffer->restoreMessages($messages);

                // Give up only when the next attempt could not start inside
                // the deadline. The first attempt always happens, so a caller
                // with a tiny maxWaitMillis still gets one real try before the
                // raise rather than an unattempted failure.
                if (microtime(true) + ($retryDelayMicros / 1000000) > $deadline) {
                    throw $error;
                }

                usleep($retryDelayMicros);
            }
        }

        if ($retireWhenDrained && $buffer->getMessageCount() === 0) {
            unset($this->buffers[$queueAddress]);
        }
    }

    public function flushBuffer(string $queueAddress): void
    {
        $buffer = $this->buffers[$queueAddress] ?? null;
        if ($buffer === null) {
            return;
        }

        $this->sendUntilDrained($queueAddress, $buffer, true);
    }

    /**
     * Flush all buffers concurrently using async HTTP.
     * Each buffer's batches are extracted up front, then all sent in parallel.
     *
     * The batches live OUTSIDE their buffers between extraction and settle,
     * which is why every failure path below has to put them back — in order,
     * into a buffer that still carries the caller's own options — before it
     * does anything else. Whatever the concurrent pass could not deliver is
     * then retried sequentially through sendUntilDrained(), so the "a failed
     * batch is re-queued and retried, never dropped" contract holds here too
     * and not only on the add path.
     */
    public function flushAllBuffers(): void
    {
        $addresses = array_keys($this->buffers);

        if (empty($addresses)) {
            return;
        }

        // Collect all batches from all buffers
        $batches = []; // [[messages, address], ...]
        $optionsByAddress = [];
        foreach ($addresses as $address) {
            $buffer = $this->buffers[$address] ?? null;
            if ($buffer === null || $buffer->getMessageCount() === 0) {
                continue;
            }

            $options = $buffer->getOptions();
            $optionsByAddress[$address] = $options;
            $batchSize = $options['messageCount'];
            $buffer->setFlushing(true);

            while ($buffer->getMessageCount() > 0) {
                $messages = $buffer->extractMessages($batchSize);
                if (!empty($messages)) {
                    $batches[] = [$messages, $address];
                }
            }
        }

        if (empty($batches)) {
            foreach ($addresses as $address) {
                unset($this->buffers[$address]);
            }
            return;
        }

        // Send all batches concurrently
        $promises = [];
        foreach ($batches as $i => [$messages, $address]) {
            $promises[$i] = $this->httpClient->postAsync('/api/v1/push', ['items' => $messages]);
        }

        $results = HttpClient::settleAll($promises);

        // Collect the failures per address, keeping the batches in the order
        // they were extracted. They have to be restored in ONE pass per
        // address: restoring them one at a time as the results are walked
        // unshifts each failed batch in front of the previous one, which
        // reverses the stream for any buffer that failed more than one batch.
        $failedByAddress = [];
        foreach ($results as $i => $outcome) {
            if ($outcome['state'] === 'fulfilled') {
                $this->flushCount++;
                continue;
            }

            [$failedMessages, $failedAddress] = $batches[$i];
            foreach ($failedMessages as $message) {
                $failedByAddress[$failedAddress][] = $message;
            }
        }

        foreach ($failedByAddress as $address => $messages) {
            if (!isset($this->buffers[$address])) {
                // Re-create with the buffer's OWN options. Re-creating it from
                // Defaults::BUFFER_DEFAULTS silently retuned a caller's
                // thresholds — a producer that asked for messageCount 5000
                // came back from a failed flush batching at 100, and its
                // maxSize bound moved with it.
                $this->buffers[$address] = new MessageBuffer(
                    $address,
                    $optionsByAddress[$address] ?? Defaults::BUFFER_DEFAULTS,
                    fn(string $addr) => $this->doFlush($addr)
                );
            }
            $this->buffers[$address]->restoreMessages($messages);
        }

        // Retry what the concurrent pass could not deliver, one address at a
        // time, each under its own maxWaitMillis deadline. A transient failure
        // that the retry recovers from is a success, so it must not raise:
        // only messages still sitting in a buffer at the end are an error.
        $retryErrors = [];
        foreach (array_keys($failedByAddress) as $address) {
            $buffer = $this->buffers[$address] ?? null;
            if ($buffer === null || $buffer->getMessageCount() === 0) {
                continue;
            }
            try {
                $this->sendUntilDrained($address, $buffer, true);
            } catch (\Throwable $error) {
                $retryErrors[] = $error;
            }
        }

        // Clean up only successful buffers
        foreach ($addresses as $address) {
            $buffer = $this->buffers[$address] ?? null;
            if ($buffer !== null && $buffer->getMessageCount() === 0) {
                unset($this->buffers[$address]);
            }
        }

        // Throw first error if any batch is still undelivered
        if (!empty($retryErrors)) {
            throw $retryErrors[0];
        }
    }

    public function getStats(): array
    {
        $totalBufferedMessages = 0;
        $oldestBufferAge = 0.0;

        foreach ($this->buffers as $buffer) {
            $totalBufferedMessages += $buffer->getMessageCount();
            $age = $buffer->getFirstMessageAge();
            $oldestBufferAge = max($oldestBufferAge, $age);
        }

        return [
            'activeBuffers' => count($this->buffers),
            'totalBufferedMessages' => $totalBufferedMessages,
            'oldestBufferAge' => $oldestBufferAge,
            'flushesPerformed' => $this->flushCount,
        ];
    }

    public function cleanup(): void
    {
        foreach ($this->buffers as $buffer) {
            $buffer->cleanup();
        }
        $this->buffers = [];
    }
}
