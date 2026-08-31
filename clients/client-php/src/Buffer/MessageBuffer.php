<?php

namespace Queen\Buffer;

use Queen\Support\Defaults;

class MessageBuffer
{
    private string $queueAddress;
    private array $messages = [];
    private array $options;
    private \Closure $flushCallback;
    private ?float $firstMessageTime = null;
    private bool $flushing = false;
    private Destination $destination;

    /**
     * @param Destination|null $destination Where this buffer's batches are
     *   posted (Buffer/Sink.php). Null means the DURABLE push — what every
     *   caller that predates ephemeral queues gets without knowing sinks exist.
     */
    public function __construct(
        string $queueAddress,
        array $options,
        \Closure $flushCallback,
        ?Destination $destination = null
    ) {
        $this->queueAddress = $queueAddress;
        $this->options = self::normalizeOptions($options);
        $this->flushCallback = $flushCallback;
        $this->destination = $destination ?? Destination::durable();
    }

    /**
     * Resolve the buffer knobs, filling in Defaults::BUFFER_DEFAULTS and
     * deriving the ones that depend on messageCount. Idempotent, and public
     * so BufferManager can read a caller's effective bound without owning a
     * second copy of these rules.
     *
     * maxSize 0 (or absent) is the bounded DEFAULT, not unbounded: opting out
     * of backpressure is not expressible, because the unbounded buffer WAS the
     * defect — see Defaults::BUFFER_DEFAULTS for the measured numbers. The
     * floor keeps maxSize sane when a caller sets a messageCount above their
     * maxSize: a bound below the flush threshold would mean the buffer is
     * "full" before it ever reaches the count that triggers a flush.
     */
    public static function normalizeOptions(array $options): array
    {
        $options = array_merge(Defaults::BUFFER_DEFAULTS, $options);

        $messageCount = (int) ($options['messageCount'] ?? 0);
        if ($messageCount <= 0) {
            $messageCount = Defaults::BUFFER_DEFAULTS['messageCount'];
        }

        $timeMillis = (int) ($options['timeMillis'] ?? 0);
        if ($timeMillis <= 0) {
            $timeMillis = Defaults::BUFFER_DEFAULTS['timeMillis'];
        }

        $maxSize = (int) ($options['maxSize'] ?? 0);
        if ($maxSize <= 0) {
            $maxSize = 4 * $messageCount;
        }
        if ($maxSize < $messageCount) {
            $maxSize = $messageCount;
        }

        $retryDelayMillis = (int) ($options['retryDelayMillis'] ?? 0);
        if ($retryDelayMillis <= 0) {
            $retryDelayMillis = Defaults::BUFFER_DEFAULTS['retryDelayMillis'];
        }

        $maxWaitMillis = (int) ($options['maxWaitMillis'] ?? 0);
        if ($maxWaitMillis <= 0) {
            $maxWaitMillis = Defaults::BUFFER_DEFAULTS['maxWaitMillis'];
        }

        return array_merge($options, [
            'messageCount' => $messageCount,
            'timeMillis' => $timeMillis,
            'maxSize' => $maxSize,
            'retryDelayMillis' => $retryDelayMillis,
            'maxWaitMillis' => $maxWaitMillis,
        ]);
    }

    /**
     * BACKPRESSURE. Before the maxSize bound existed, this method appended to
     * an unbounded array and returned — reporting success for messages that
     * only ever lived in this process's heap, and dying with them. At the
     * bound the add path must NOT simply return: it flushes inline, right
     * here, and the flusher (BufferManager) retries a failed batch until it
     * lands or its deadline expires.
     *
     * Why inline rather than parking: PHP is synchronous and single-threaded
     * per request, so there is no background flusher whose progress we could
     * wait on — waiting would deadlock against the very code that drains the
     * buffer. This is the same accommodation checkTimeTrigger() already makes
     * for the time-based flush, which the other SDKs get from a timer.
     */
    public function add(array $formattedMessage): void
    {
        if (count($this->messages) >= $this->options['maxSize']) {
            // A buffer sitting at the bound is exactly the state the flushing
            // latch must not survive: the latch suppresses every later count
            // trigger, so a latched full buffer would grow forever without one
            // more send attempt. Clear it and make the attempt.
            $this->flushing = false;
            $this->triggerFlush();

            // The flusher either drained the buffer or threw. Landing here
            // still full means the flush callback returned without draining
            // (a caller-supplied sink that quietly declines). Accepting the
            // message anyway is the defect we are removing: raise instead, so
            // no caller is ever told a message was taken when it was not.
            if (count($this->messages) >= $this->options['maxSize']) {
                throw new \RuntimeException(sprintf(
                    'Queen buffer for %s is full (%d/%d messages) and the inline flush did not drain it; '
                    . 'the message was NOT accepted. Slow down the producer, raise maxSize, or fix the flush sink.',
                    $this->queueAddress,
                    count($this->messages),
                    $this->options['maxSize']
                ));
            }
        }

        if (empty($this->messages)) {
            $this->firstMessageTime = microtime(true);
        }

        $this->messages[] = $formattedMessage;

        // Check if we should flush based on count. Note the message is already
        // buffered at this point, so a raise from here means "the flush failed
        // and could not be retried into the deadline", not "your message was
        // rejected" — it is still queued and a later flush will send it. Same
        // shape as the Go SDK, where Add appends and then returns Flush's
        // error. A caller who reacts by re-pushing gets broker-side
        // transaction-id dedup, which is why erring on the side of telling
        // them is right: the alternative is silence.
        if (count($this->messages) >= $this->options['messageCount']) {
            $this->triggerFlush();
        }
    }

    /**
     * Check time-based flush trigger. Call this periodically.
     */
    public function checkTimeTrigger(): void
    {
        if ($this->flushing || empty($this->messages) || $this->firstMessageTime === null) {
            return;
        }

        $elapsedMs = (microtime(true) - $this->firstMessageTime) * 1000;
        if ($elapsedMs >= $this->options['timeMillis']) {
            $this->triggerFlush();
        }
    }

    private function triggerFlush(): void
    {
        if ($this->flushing || empty($this->messages)) {
            return;
        }

        $this->flushing = true;

        try {
            ($this->flushCallback)($this->queueAddress);
        } catch (\Throwable $error) {
            // The flusher restores a failed batch before it rethrows, and
            // restoreMessages() clears the latch. A callback that fails some
            // other way must not leave the buffer latched as "flushing":
            // the latch suppresses every later count trigger, so a latched
            // buffer grows without ever attempting another flush — the
            // unbounded-growth defect by a different route. Only clear it when
            // messages are actually still here, so a callback that drained the
            // buffer and then threw keeps the normal post-drain state.
            if (!empty($this->messages)) {
                $this->flushing = false;
            }
            throw $error;
        }
    }

    public function extractMessages(?int $batchSize = null): array
    {
        if ($batchSize === null || $batchSize >= count($this->messages)) {
            $messages = $this->messages;
            $this->messages = [];
            $this->firstMessageTime = null;
            $this->flushing = false;
            return $messages;
        }

        $messages = array_splice($this->messages, 0, $batchSize);

        if (empty($this->messages)) {
            $this->firstMessageTime = null;
            $this->flushing = false;
        }

        return $messages;
    }

    /**
     * Restore messages to the front of the buffer (used on flush failure).
     *
     * The FRONT, and in their original order, is the whole point: a batch that
     * failed to send is the oldest thing in the buffer, so putting it back
     * anywhere else would reorder the stream against the producer's intent.
     * The caller (BufferManager) then retries this same batch after
     * retryDelayMillis rather than dropping it.
     */
    public function restoreMessages(array $messages): void
    {
        array_unshift($this->messages, ...$messages);
        if ($this->firstMessageTime === null && !empty($this->messages)) {
            $this->firstMessageTime = microtime(true);
        }
        $this->flushing = false;
    }

    public function setFlushing(bool $value): void
    {
        $this->flushing = $value;
    }

    public function getMessageCount(): int
    {
        return count($this->messages);
    }

    public function getMaxSize(): int
    {
        return $this->options['maxSize'];
    }

    /**
     * At or past the backpressure bound. Occupancy can sit exactly ON the
     * bound (the add path refuses to grow past it), never above it: unlike the
     * Go SDK, where a concurrent flusher can re-queue a failed batch while a
     * producer is parked and overshoot by one batch, everything here happens on
     * one thread, so the re-queue always lands before the next bound check.
     */
    public function isFull(): bool
    {
        return count($this->messages) >= $this->options['maxSize'];
    }

    public function getOptions(): array
    {
        return $this->options;
    }

    /** Where this buffer's batches are posted, and in what envelope. */
    public function getDestination(): Destination
    {
        return $this->destination;
    }

    public function getFirstMessageAge(): float
    {
        return $this->firstMessageTime !== null
            ? (microtime(true) - $this->firstMessageTime) * 1000
            : 0;
    }

    public function cleanup(): void
    {
        $this->messages = [];
        $this->firstMessageTime = null;
        $this->flushing = false;
    }
}
