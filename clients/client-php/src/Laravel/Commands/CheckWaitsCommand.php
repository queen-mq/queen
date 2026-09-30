<?php

namespace Queen\Laravel\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Notification;
use Queen\Laravel\Events\LongWaitDetected;
use Queen\Laravel\Monitoring\QueueWaits;
use Queen\Laravel\Notifications\LongWaitDetected as LongWaitNotification;

/**
 * Horizon's long-wait notifications for Queen: schedule it every minute.
 *
 * For every `connection:queue` in `queen.waits`, and every consumer group a
 * supervisor pool consumes it with, it measures the oldest waiting job
 * (QueueWaits). Above the threshold it dispatches LongWaitDetected and, when
 * `queen.notifications.mail` is set, mails it, at most once per queue and
 * group per `queen.notifications.throttle_minutes`.
 */
class CheckWaitsCommand extends Command
{
    protected $signature = 'queen:check-waits';

    protected $description = 'Report Queen queues whose oldest job waits longer than its threshold';

    public function handle(Dispatcher $events): int
    {
        $config = $this->laravel['config'];
        $waits = $config->get('queen.waits', []);
        if (!is_array($waits) || $waits === []) {
            $this->components->info('No queue waits are configured (queen.waits).');

            return self::SUCCESS;
        }

        $failed = false;
        $readers = [];
        foreach ($waits as $target => $threshold) {
            [$connection, $queue] = str_contains((string) $target, ':')
                ? explode(':', (string) $target, 2)
                : ['queen', (string) $target];
            if (!is_int($threshold) && !(is_string($threshold) && ctype_digit($threshold))) {
                $this->components->error("queen.waits [{$target}] must be a number of seconds.");
                $failed = true;
                continue;
            }
            $threshold = (int) $threshold;
            try {
                $readers[$connection] ??= $this->laravel->make(QueueWaits::class, ['connection' => $connection]);
                foreach ($this->consumerGroups($connection, $queue) as $group) {
                    $seconds = $readers[$connection]->seconds($queue, $group);
                    if ($seconds < $threshold) {
                        continue;
                    }
                    $wait = new LongWaitDetected($connection, $queue, $group, $seconds, $threshold);
                    $this->components->warn("{$connection}:{$queue} ({$group}) waits {$seconds}s, threshold {$threshold}s.");
                    if ($this->throttled($wait)) {
                        continue;
                    }
                    try {
                        $events->dispatch($wait);
                        $this->notify($wait);
                    } catch (\Throwable $error) {
                        // Not sent: the next run tries again.
                        $this->release($wait);
                        throw $error;
                    }
                }
            } catch (\Throwable $error) {
                $this->components->error("{$connection}:{$queue}: {$error->getMessage()}");
                $failed = true;
            }
        }

        return $failed ? self::FAILURE : self::SUCCESS;
    }

    /** @return list<string> the consumer groups supervisor pools consume this queue with */
    private function consumerGroups(string $connection, string $queue): array
    {
        $config = $this->laravel['config'];
        $groups = [];
        foreach ((array) $config->get('queen.supervisor.supervisors', []) as $options) {
            if (!is_array($options) || (string) ($options['connection'] ?? 'queen') !== $connection) {
                continue;
            }
            $queues = $options['queues'] ?? $options['queue'] ?? [$config->get('queen.queue', 'default')];
            $queues = is_string($queues) ? array_map('trim', explode(',', $queues)) : (array) $queues;
            if (in_array($queue, $queues, true)) {
                $groups[] = (string) ($options['consumer_group'] ?? $config->get('queen.consumer_group', 'laravel'));
            }
        }

        return $groups !== [] ? array_values(array_unique($groups)) : [(string) $config->get('queen.consumer_group', 'laravel')];
    }

    private function throttled(LongWaitDetected $wait): bool
    {
        $minutes = (int) $this->laravel['config']->get('queen.notifications.throttle_minutes', 5);
        if ($minutes <= 0 || !$this->laravel->bound('cache')) {
            return false;
        }
        /** @var Cache $cache */
        $cache = $this->laravel['cache']->store();

        return !$cache->add(self::throttleKey($wait), true, $minutes * 60);
    }

    private function release(LongWaitDetected $wait): void
    {
        try {
            $this->laravel['cache']->store()->forget(self::throttleKey($wait));
        } catch (\Throwable) {
            // The throttle window expires on its own.
        }
    }

    private static function throttleKey(LongWaitDetected $wait): string
    {
        return 'queen:long-wait:' . sha1("{$wait->connection}\0{$wait->queue}\0{$wait->consumerGroup}");
    }

    private function notify(LongWaitDetected $wait): void
    {
        $mail = $this->laravel['config']->get('queen.notifications.mail');
        if (is_string($mail) && $mail !== '') {
            Notification::route('mail', array_map('trim', explode(',', $mail)))->notify(new LongWaitNotification($wait));
        }
    }

}
