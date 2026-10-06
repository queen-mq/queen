<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Queen\Consumer\HighLevelConsumer;
use Queen\Queen;

class ConsumeCommand extends Command
{
    protected $signature = 'queen:consume
        {queue : Queue name to consume from}
        {handler : Fully qualified class name with handle() method}
        {--group= : Consumer group name}
        {--batch= : Messages per pop, default 1. Above 1, handle() receives a list of messages}
        {--partitions= : Partitions to claim per pop. Omit and the broker sizes it; --partitions=1 pins the legacy single-partition claim}
        {--no-autopilot : Restore the pre-1.2 client-side defaults (batch 1, partitions 1) and send no autopilot parameter}
        {--auto-ack : Ack each message (or batch) when handle() returns. A handler that throws is nacked either way}
        {--subscription-mode= : Subscription mode}
        {--subscription-from= : Subscription start point}
        {--conflation : Last-value delivery: process only the newest message per partition (needs --group, broker >= 1.1.0)}
        {--timeout=30000 : Long poll timeout in milliseconds}
        {--idle-timeout= : Stop after N milliseconds of inactivity}
        {--limit= : Stop after handing N messages to handle(), failed ones included}';

    protected $description = 'Consume messages from a Queen MQ queue';

    /**
     * The broker's answers for an item that an earlier ack already settled
     * (server/src/rsm/facade/real.rs, the ack render): its lease is released,
     * or its transaction is below the cursor.
     */
    private const ALREADY_SETTLED = [
        'invalid or expired lease',
        'transaction is unresolvable, already committed, or acknowledgment is stale',
    ];

    public function handle(Queen $queen): int
    {
        $queueName = $this->argument('queue');
        $handlerClass = $this->argument('handler');

        if (!class_exists($handlerClass)) {
            $this->error("Handler class not found: {$handlerClass}");
            return self::FAILURE;
        }

        $handler = app($handlerClass);
        if (!method_exists($handler, 'handle')) {
            $this->error("Handler class must have a handle() method: {$handlerClass}");
            return self::FAILURE;
        }

        $this->info("Starting Queen consumer on queue: {$queueName}");

        $builder = $queen->queue($queueName);

        // Pop autopilot: an option the operator TYPED is a pin, an option left
        // out is the broker's to choose. A default of 1 here would have pinned
        // both knobs on every run without anyone asking, which is the whole
        // feature switched off by accident.
        if ($this->option('batch') !== null) {
            $builder->batch((int) $this->option('batch'));
        }

        if ($this->option('partitions') !== null) {
            $builder->partitions((int) $this->option('partitions'));
        }

        if ($this->option('no-autopilot')) {
            $builder->autopilot(false);
        }

        if ($this->option('group')) {
            $builder->group($this->option('group'));
            $this->info("Consumer group: {$this->option('group')}");
        }

        if ($this->option('auto-ack')) {
            $builder->autoAck(true);
        }

        if ($this->option('subscription-mode')) {
            $builder->subscriptionMode($this->option('subscription-mode'));
        }

        if ($this->option('subscription-from')) {
            $builder->subscriptionFrom($this->option('subscription-from'));
        }

        if ($this->option('conflation')) {
            $builder->conflation(true);
            $this->info('Conflation: on (only the newest message per partition is processed)');
        }

        if ($this->option('idle-timeout')) {
            $builder->idleMillis((int) $this->option('idle-timeout'));
        }

        // Use the high-level consumer (rdkafka-style)
        $consumer = $builder->getConsumer();
        $consumer->subscribe();

        $this->info('Consumer subscribed. Waiting for messages... (Ctrl+C to stop)');

        // Every message handed to handle() counts, failed or not, so --limit
        // bounds the work the command takes on rather than its successes.
        $processed = 0;
        // Which consume surface to drive, not what to put on the wire: the
        // sizing already reached the builder above. An operator who named no
        // batch gets the one-message loop, exactly as before -- the broker's
        // choice of batch is about the claim, and this loop hands the caller one
        // message at a time either way.
        $batch = $this->option('batch') !== null ? (int) $this->option('batch') : 1;
        $autoAck = (bool) $this->option('auto-ack');
        $timeout = (int) $this->option('timeout');
        $limit = $this->option('limit') ? (int) $this->option('limit') : null;

        while (!$consumer->isClosed()) {
            if ($batch > 1) {
                // Never claim more than the limit leaves: a message popped
                // past it would sit leased until its lease ran out.
                $wanted = $limit === null ? $batch : min($batch, $limit - $processed);
                $messages = $consumer->consumeBatch($timeout, $wanted);
            } else {
                $message = $consumer->consume($timeout);
                $messages = $message === null ? [] : [$message];
            }
            if ($messages === []) {
                continue;
            }

            $processed += count($messages);
            $this->handOut($consumer, $handler, $messages, $batch > 1, $autoAck);

            if ($limit !== null && $processed >= $limit) {
                $this->info("Message limit reached ({$limit})");
                break;
            }
        }

        $consumer->close();
        $this->info("Consumer stopped. Processed {$processed} messages.");

        return self::SUCCESS;
    }

    /**
     * Run the handler on one pop's messages, then settle them: a nack when it
     * threw, with or without --auto-ack, so a failure spends a retry and a
     * message that always fails reaches the dead-letter queue; an ack when it
     * returned, with --auto-ack only.
     *
     * @param list<array> $messages
     */
    private function handOut(
        HighLevelConsumer $consumer,
        object $handler,
        array $messages,
        bool $asList,
        bool $autoAck,
    ): void {
        $payload = $asList ? $messages : $messages[0];
        try {
            $handler->handle($payload);
        } catch (\Throwable $e) {
            $this->error(($asList ? 'Error processing batch: ' : 'Error processing message: ') . $e->getMessage());
            $this->settle($consumer, $payload, count($messages), false, $e->getMessage());

            return;
        }

        if ($autoAck) {
            $this->settle($consumer, $payload, count($messages), true, null);
        }
    }

    /** One ack or nack call, and one warning line when the broker refused it. */
    private function settle(HighLevelConsumer $consumer, array $payload, int $count, bool $success, ?string $error): void
    {
        $result = $success ? $consumer->ack($payload) : $consumer->nack($payload, $error);
        $verb = $success ? 'Ack' : 'Nack';
        $noun = $count === 1 ? 'message' : 'messages';

        if (($result['success'] ?? false) !== true) {
            $this->warn("{$verb} failed for {$count} {$noun}: " . (string) ($result['error'] ?? 'no answer'));

            return;
        }

        $refused = 0;
        $reason = null;
        foreach ($result as $key => $item) {
            if (!is_int($key) || !is_array($item) || ($item['success'] ?? true) !== false) {
                continue;
            }
            $itemError = (string) ($item['error'] ?? 'refused');
            // A handler may ack or nack by itself before it throws. The nack
            // that follows then finds the message settled and its lease
            // released, which is no news: the handler's own call won.
            if (!$success && in_array($itemError, self::ALREADY_SETTLED, true)) {
                continue;
            }
            ++$refused;
            $reason ??= $itemError;
        }

        if ($refused > 0) {
            $this->warn("{$verb} refused for {$refused} of {$count} {$noun}: {$reason}");
        }
    }
}
