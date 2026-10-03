<?php

namespace App\Notifications;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Notifications\Notification;

/** A queued notification on a custom channel: Laravel queues one job per notifiable and channel. */
final class CompatNotification extends Notification implements ShouldQueue
{
    use Queueable;

    public function __construct(public string $runId)
    {
    }

    /** @return list<class-string> */
    public function via(object $notifiable): array
    {
        return [CompatChannel::class];
    }

    /** @return array{run_id: string, job_id: string} */
    public function toCompat(object $notifiable): array
    {
        return ['run_id' => $this->runId, 'job_id' => 'n' . $notifiable->getKey()];
    }
}
