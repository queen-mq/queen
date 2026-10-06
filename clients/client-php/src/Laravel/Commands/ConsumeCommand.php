<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Carbon;
use Illuminate\Support\Sleep;
use InvalidArgumentException;
use Queen\Consumer\HighLevelConsumer;
use Queen\Laravel\QueenServiceProvider;
use Queen\Laravel\Queue\LeaseRenewer;
use Queen\Laravel\Queue\LeaseRenewerFactory;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Queen;
use RuntimeException;

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
        {--lease= : Lease of each pop in seconds. Default: retry_after in config/queen.php}
        {--subscription-mode= : Subscription mode}
        {--subscription-from= : Subscription start point}
        {--conflation : Last-value delivery: process only the newest message per partition (needs --group, broker >= 1.1.0)}
        {--timeout=30000 : Long poll timeout in milliseconds}
        {--idle-timeout= : Stop, with exit code 0, after N milliseconds without a message}
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

    /** The wait after a pop that reached no broker. */
    private const RECONNECT_DELAY_MICROS = 1_000_000;

    /** While the broker stays unreachable, one more warning at most this often. */
    private const UNREACHABLE_WARNING_INTERVAL_MILLIS = 30_000;

    /** Wall-clock ms when the pops began to fail to connect; null while they get answers. */
    private ?int $unreachableSince = null;

    private ?int $lastUnreachableWarning = null;

    /** Renews the lease of the messages in hand; null unless lease_renewal and --auto-ack. */
    private ?LeaseRenewer $renewer = null;

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

        $autoAck = (bool) $this->option('auto-ack');
        try {
            [$lease, $leaseName] = $this->lease();
            $this->renewer = $this->leaseRenewer($lease, $leaseName, $autoAck);
        } catch (InvalidArgumentException $invalid) {
            $this->error($invalid->getMessage());
            return self::FAILURE;
        }

        $builder = $queen->queue($queueName)->leaseSeconds($lease);

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

        // Use the high-level consumer (rdkafka-style)
        $consumer = $builder->getConsumer();
        $consumer->subscribe();

        $this->info('Consumer subscribed. Waiting for messages... (Ctrl+C to stop)');

        try {
            $processed = $this->consume($consumer, $handler, $lease, $autoAck);
        } finally {
            $this->renewer?->close();
            $consumer->close();
        }

        $this->info("Consumer stopped. Processed {$processed} messages.");

        return self::SUCCESS;
    }

    /** Pop, hand out and settle until a signal, --limit or --idle-timeout; returns the messages handed out. */
    private function consume(HighLevelConsumer $consumer, object $handler, int $lease, bool $autoAck): int
    {
        // Every message handed to handle() counts, failed or not, so --limit
        // bounds the work the command takes on rather than its successes.
        $processed = 0;
        // Which consume surface to drive, not what to put on the wire: the
        // sizing already reached the builder. Without --batch the command
        // drives consume(), which pins batch=1, and hands out one message.
        $batch = $this->option('batch') !== null ? (int) $this->option('batch') : 1;
        $timeout = (int) $this->option('timeout');
        $limit = $this->option('limit') ? (int) $this->option('limit') : null;
        // HighLevelConsumer has no idle stop, so the command keeps the clock:
        // from the start, then from the end of the last handle().
        $idleMillis = $this->option('idle-timeout') ? (int) $this->option('idle-timeout') : null;
        $lastMessageAt = self::monotonicMillis();

        while (!$consumer->isClosed()) {
            $popTimeout = $timeout;
            if ($idleMillis !== null) {
                $idleLeft = $idleMillis - (self::monotonicMillis() - $lastMessageAt);
                if ($idleLeft <= 0) {
                    $this->info("No message for {$idleMillis} ms (--idle-timeout): stopping.");
                    break;
                }
                // A long poll must not outlast the idle deadline.
                $popTimeout = min($timeout, $idleLeft);
            }

            // The broker starts the lease inside this request. A deadline
            // counted from before it can only fence early, never renew past
            // the broker's own expiry.
            $popStartedAt = self::monotonicMillis();
            if ($batch > 1) {
                // Never claim more than the limit leaves: a message popped
                // past it would sit leased until its lease ran out.
                $wanted = $limit === null ? $batch : min($batch, $limit - $processed);
                $messages = $consumer->consumeBatch($popTimeout, $wanted);
            } else {
                $message = $consumer->consume($popTimeout);
                $messages = $message === null ? [] : [$message];
            }
            $this->watchBroker($consumer);
            if ($messages === []) {
                // A pop that reached no broker comes back at once, after one
                // attempt per URL: wait before the next one, as consume()'s
                // own loop does, instead of polling in a tight loop.
                if ($consumer->lastPopError() !== null) {
                    Sleep::usleep(self::RECONNECT_DELAY_MICROS);
                }
                continue;
            }

            $processed += count($messages);
            $leases = $this->trackLeases($messages, $popStartedAt + $lease * 1000);
            $this->handOut($consumer, $handler, $messages, $batch > 1, $autoAck, $leases);
            $lastMessageAt = self::monotonicMillis();

            if ($limit !== null && $processed >= $limit) {
                $this->info("Message limit reached ({$limit})");
                break;
            }
        }

        return $processed;
    }

    /**
     * The lease of every pop: --lease, else retry_after of config/queen.php.
     *
     * @return array{0: int, 1: string} the seconds, and the name errors give them
     */
    private function lease(): array
    {
        $option = $this->option('lease');
        $name = $option !== null ? '--lease' : 'retry_after';

        return [
            QueenConnector::boundedInteger(
                $option ?? config('queen.retry_after', 90),
                $name,
                1,
                QueenConnector::MAX_RETRY_AFTER_SECONDS,
            ),
            $name,
        ];
    }

    /**
     * The renewer queue:work would use, when config/queen.php turns
     * lease_renewal on. Only with --auto-ack: without it the handler may ack
     * by itself, and a renewer that goes on renewing the lease that ack
     * released fails, and fences (kills) this process.
     */
    private function leaseRenewer(int $lease, string $leaseName, bool $autoAck): ?LeaseRenewer
    {
        if (config('queen.lease_renewal') !== true) {
            return null;
        }
        if (!$autoAck) {
            $this->warn('Lease renewal is off: queen:consume needs --auto-ack to renew leases, since a handler that acks by itself releases its lease.');

            return null;
        }

        $config = (array) config('queen', []);
        $timing = LeaseRenewerFactory::timing($config, $lease);
        $renewer = app(LeaseRenewerFactory::class)->make(
            QueenServiceProvider::clientConfig($config),
            $lease,
            $timing,
            $leaseName,
        );
        $this->info("Lease renewal: on, every {$timing['interval']} s, for a lease of {$lease} s.");

        return $renewer;
    }

    /**
     * Start renewing the lease of the messages just popped (one pop, one
     * lease), until they are settled.
     *
     * @param list<array> $messages
     * @return list<string> the lease IDs tracked
     */
    private function trackLeases(array $messages, int $deadlineMonotonicMillis): array
    {
        if ($this->renewer === null) {
            return [];
        }

        $leases = [];
        foreach ($messages as $message) {
            $leaseId = $message['leaseId'] ?? null;
            if (!is_string($leaseId) || $leaseId === '') {
                throw new RuntimeException('Queen returned a message without a lease ID while lease renewal is enabled.');
            }
            $leases[$leaseId] = true;
        }

        $tracked = [];
        try {
            foreach (array_keys($leases) as $leaseId) {
                $this->renewer->track($leaseId, $deadlineMonotonicMillis);
                $tracked[] = $leaseId;
            }
        } catch (\Throwable $exception) {
            foreach ($tracked as $leaseId) {
                $this->renewer->forget($leaseId);
            }
            // A failed track has an ambiguous outcome on the helper's side:
            // close it so no unconfirmed lease is renewed as an orphan.
            $this->renewer->close();
            throw $exception;
        }

        return $tracked;
    }

    /**
     * Run the handler on one pop's messages, then settle them: a nack when it
     * threw, with or without --auto-ack, so a failure spends a retry and a
     * message that always fails reaches the dead-letter queue; an ack when it
     * returned, with --auto-ack only.
     *
     * @param list<array> $messages
     * @param list<string> $leases tracked by the renewer
     */
    private function handOut(
        HighLevelConsumer $consumer,
        object $handler,
        array $messages,
        bool $asList,
        bool $autoAck,
        array $leases,
    ): void {
        $payload = $asList ? $messages : $messages[0];
        try {
            $handler->handle($payload);
        } catch (\Throwable $e) {
            $this->error(($asList ? 'Error processing batch: ' : 'Error processing message: ') . $e->getMessage());
            $this->settle($consumer, $payload, count($messages), false, $e->getMessage(), $leases);

            return;
        }

        if ($autoAck) {
            $this->settle($consumer, $payload, count($messages), true, null, $leases);
        }
    }

    /**
     * One ack or nack call, and one warning line when the broker refused it.
     * A renewed lease is checked and forgotten first: a lease the renewer can
     * no longer vouch for is never settled, and one the ack releases must not
     * be renewed after it.
     *
     * @param list<string> $leases
     */
    private function settle(
        HighLevelConsumer $consumer,
        array $payload,
        int $count,
        bool $success,
        ?string $error,
        array $leases,
    ): void {
        $verb = $success ? 'Ack' : 'Nack';
        $unsafe = $this->releaseLeases($leases);
        if ($unsafe !== null) {
            $this->warn('Not ' . strtolower($verb) . "ed: {$unsafe}. The broker hands the message out again after its lease.");

            return;
        }

        $result = $success ? $consumer->ack($payload) : $consumer->nack($payload, $error);
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

    /**
     * Check, then forget, every tracked lease.
     *
     * @param list<string> $leases
     * @return string|null why a lease is no longer safe to settle, or null
     */
    private function releaseLeases(array $leases): ?string
    {
        if ($this->renewer === null) {
            return null;
        }

        $unsafe = null;
        foreach ($leases as $leaseId) {
            try {
                $this->renewer->assertHealthy($leaseId);
            } catch (\Throwable $exception) {
                $unsafe ??= $exception->getMessage();
            }
            $this->renewer->forget($leaseId);
        }

        return $unsafe;
    }

    /**
     * Say when the pops stop reaching the broker, at most every 30 s while
     * that lasts, and once when it answers again: HighLevelConsumer returns a
     * connection error as an empty pop, so without this the output of a
     * consumer cut off from its broker reads as a quiet queue.
     */
    private function watchBroker(HighLevelConsumer $consumer): void
    {
        $error = $consumer->lastPopError();
        $now = Carbon::now()->getTimestampMs();

        if ($error === null) {
            if ($this->unreachableSince !== null) {
                $seconds = intdiv($now - $this->unreachableSince, 1000);
                $this->info("Queen broker reachable again after {$seconds} s.");
                $this->unreachableSince = null;
                $this->lastUnreachableWarning = null;
            }

            return;
        }

        if ($this->unreachableSince === null) {
            $this->unreachableSince = $now;
            $this->lastUnreachableWarning = $now;
            $this->warn("Queen broker unreachable: {$error}. Still polling.");

            return;
        }

        if ($now - $this->lastUnreachableWarning >= self::UNREACHABLE_WARNING_INTERVAL_MILLIS) {
            $this->lastUnreachableWarning = $now;
            $seconds = intdiv($now - $this->unreachableSince, 1000);
            $this->warn("Queen broker still unreachable after {$seconds} s: {$error}");
        }
    }

    private static function monotonicMillis(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }
}
