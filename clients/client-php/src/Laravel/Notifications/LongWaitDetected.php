<?php

namespace Queen\Laravel\Notifications;

use Illuminate\Notifications\Messages\MailMessage;
use Illuminate\Notifications\Notification;
use Queen\Laravel\Events\LongWaitDetected as LongWait;

final class LongWaitDetected extends Notification
{
    public function __construct(public readonly LongWait $wait)
    {
    }

    /** @return list<string> */
    public function via(mixed $notifiable): array
    {
        return ['mail'];
    }

    public function toMail(mixed $notifiable): MailMessage
    {
        $wait = $this->wait;

        return (new MailMessage())
            ->error()
            ->subject("Long wait on queue {$wait->queue}")
            ->line("Jobs on queue {$wait->queue} (connection {$wait->connection}, consumer group {$wait->consumerGroup}) have waited {$wait->seconds} seconds, above the {$wait->threshold}-second threshold.")
            ->line('Check the supervisor dashboard: are workers running, and is capacity below the backlog?');
    }
}
