<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Queen\Queen;

class ConsumeCommand extends Command
{
    protected $signature = 'queen:consume
        {queue : Queue name to consume from}
        {handler : Fully qualified class name with handle() method}
        {--group= : Consumer group name}
        {--batch=1 : Number of messages per batch}
        {--auto-ack : Enable auto-acknowledgment}
        {--subscription-mode= : Subscription mode}
        {--subscription-from= : Subscription start point}
        {--conflation : Last-value delivery: process only the newest message per partition (needs --group, refuses --auto-ack, broker >= 1.1.0)}
        {--timeout=30000 : Long poll timeout in milliseconds}
        {--idle-timeout= : Stop after N milliseconds of inactivity}
        {--limit= : Stop after processing N messages}';

    protected $description = 'Consume messages from a Queen MQ queue';

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

        $builder = $queen->queue($queueName)
            ->batch((int) $this->option('batch'));

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

        if ($this->option('limit')) {
            $builder->limit((int) $this->option('limit'));
        }

        // Use the high-level consumer (rdkafka-style)
        $consumer = $builder->getConsumer();
        $consumer->subscribe();

        $this->info('Consumer subscribed. Waiting for messages... (Ctrl+C to stop)');

        $processed = 0;
        $batch = (int) $this->option('batch');
        $autoAck = $this->option('auto-ack');
        $timeout = (int) $this->option('timeout');
        $limit = $this->option('limit') ? (int) $this->option('limit') : null;

        while (!$consumer->isClosed()) {
            if ($batch > 1) {
                $messages = $consumer->consumeBatch($timeout, $batch);
                if (empty($messages)) {
                    continue;
                }

                try {
                    $handler->handle($messages);
                    if ($autoAck) {
                        $consumer->ack($messages);
                    }
                    $processed += count($messages);
                } catch (\Throwable $e) {
                    $this->error("Error processing batch: {$e->getMessage()}");
                    if ($autoAck) {
                        $consumer->nack($messages);
                    }
                }
            } else {
                $message = $consumer->consume($timeout);
                if ($message === null) {
                    continue;
                }

                try {
                    $handler->handle($message);
                    if ($autoAck) {
                        $consumer->ack($message);
                    }
                    $processed++;
                } catch (\Throwable $e) {
                    $this->error("Error processing message: {$e->getMessage()}");
                    if ($autoAck) {
                        $consumer->nack($message);
                    }
                }
            }

            if ($limit !== null && $processed >= $limit) {
                $this->info("Message limit reached ({$limit})");
                break;
            }
        }

        $consumer->close();
        $this->info("Consumer stopped. Processed {$processed} messages.");

        return self::SUCCESS;
    }
}
